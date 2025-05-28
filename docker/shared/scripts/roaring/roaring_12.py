import os, sys, time
import duckdb, pyarrow.parquet as pq, pandas as pd
from functools import reduce
from pyroaring import BitMap
from datafusion import SessionContext

from common_roaring import (
    bitmap_memory_size,
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
    aggregate_metrics,         
)
from common import measure_query_execution

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.makedirs("../results", exist_ok=True)

BATCH          = 6000000
SHIPMODES      = {"MAIL", "SHIP"}
R_FROM, R_TO   = pd.to_datetime("1994-01-01"), pd.to_datetime("1995-01-01")


def or_reduce(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_orders(path):
    idx, rows, bytes_, t0 = {}, 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b)
        bytes_ += df["o_orderkey"].memory_usage(deep=True)
        for key, loc in zip(df["o_orderkey"], df.index):
            idx.setdefault(key, BitMap()).add(loc + rows)
        rows += n
    return idx, rows, bytes_, time.perf_counter() - t0


def index_line(path):
    idx_ship = {}
    bm_commit_lt_receipt = BitMap()
    bm_ship_lt_commit   = BitMap()
    bm_receipt_range    = BitMap()

    rows, bytes_, t0 = 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b)

        for col in ("l_commitdate", "l_receiptdate", "l_shipdate"):
            df[col] = pd.to_datetime(df[col])
            bytes_ += df[col].memory_usage(deep=True)

        bytes_ += df["l_shipmode"].memory_usage(deep=True)

        for v in df["l_shipmode"].unique():
            idx_ship.setdefault(v, BitMap()).update(i + rows
                                                    for i in df.index[df["l_shipmode"] == v])

        bm_commit_lt_receipt.update(
            (df.index[df["l_commitdate"] < df["l_receiptdate"]] + rows).tolist()
        )
        bm_ship_lt_commit.update(
            (df.index[df["l_shipdate"]  < df["l_commitdate"]] + rows).tolist()
        )
        bm_receipt_range.update(
            (df.index[(df["l_receiptdate"] >= R_FROM) & (df["l_receiptdate"] < R_TO)] + rows).tolist()
        )

        rows += n

    return (idx_ship, bm_commit_lt_receipt, bm_ship_lt_commit,
            bm_receipt_range, rows, bytes_, time.perf_counter() - t0)


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want, cur, offset, out = iter(sorted(bitmap)), None, 0, []
    cur = next(want, None)

    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n, loc = len(b), []
        while cur is not None and cur < offset + n:
            loc.append(cur - offset)
            cur = next(want, None)
        if loc:
            out.append(b.to_pandas()[cols].iloc[loc])
        offset += n

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def save_parquet(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prep_duck(o_df, l_df, sql):
    do = save_parquet(o_df, "../data/tpch/parquet/filtered_orders.parquet")
    dl = save_parquet(l_df, "../data/tpch/parquet/filtered_lineitem.parquet")
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{do}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dl}')")
    return con, open(sql).read()


def prep_df(o_df, l_df, sql):
    do = save_parquet(o_df, "../data/tpch/parquet/filtered_orders.parquet")
    dl = save_parquet(l_df, "../data/tpch/parquet/filtered_lineitem.parquet")
    ctx = SessionContext()
    ctx.register_parquet("orders",   do)
    ctx.register_parquet("lineitem", dl)
    return ctx, open(sql).read()


if __name__ == "__main__":
    PATH_O = "../data/tpch/parquet/orders.parquet"
    PATH_L = "../data/tpch/parquet/lineitem.parquet"

    idx_okey, rows_o, bytes_o, sec_o = index_orders(PATH_O)
    (idx_ship, bm_clt, bm_slt, bm_rng,
     rows_l, bytes_l, sec_l)         = index_line(PATH_L)

    shipmode_bm = or_reduce([idx_ship[m] for m in SHIPMODES if m in idx_ship])

    bm_line   = measure_query_execution(
        lambda: shipmode_bm & bm_clt & bm_slt & bm_rng
    )
    bm_orders = measure_query_execution(
        lambda: or_reduce([idx_okey[k] for k in idx_okey
                           if (idx_okey[k] & bm_line["result"])])
    )

    df_o = materialise(PATH_O, bm_orders["result"],
                       ["o_orderkey", "o_orderpriority"])
    df_l = materialise(PATH_L, bm_line["result"],
                       ["l_orderkey", "l_shipmode",
                        "l_commitdate", "l_receiptdate", "l_shipdate"])

    bitmap_mb   = bitmap_memory_size(
        idx_okey, idx_ship,
        {"cmp": bm_clt}, {"slt": bm_slt}, {"rng": bm_rng}
    )
    original_mb = (bytes_o + bytes_l) / (1024 * 1024)
    build_secs  = sec_o + sec_l

    con, q_duck = prep_duck(df_o, df_l, "../data/tpch/queries/12.sql")
    eng_duck    = measure_query_duckdb(12, con, q_duck)

    res_duck = aggregate_metrics(bm_orders, bm_line, eng_duck)  
    res_duck.update({
        "Query": 12,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_duck.pop("result", None); res_duck.pop("error", None)

    ctx, q_df  = prep_df(df_o, df_l, "../data/tpch/queries/12.sql")
    eng_df     = measure_query_datafusion(12, ctx, q_df)

    res_df = aggregate_metrics(bm_orders, bm_line, eng_df)      
    res_df.update({
        "Query": 12,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_df.pop("result", None); res_df.pop("error", None)

    HEADERS = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)",
    ]
    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, HEADERS, [res_duck])
    write_csv_results(out_df,   HEADERS, [res_df])
