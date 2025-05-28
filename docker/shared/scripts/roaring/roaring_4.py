import os, sys, time
import duckdb, pyarrow.parquet as pq, pandas as pd
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

BATCH = 6000000
DATE_FROM = pd.to_datetime("1993-07-01")
DATE_TO   = pd.to_datetime("1993-10-01")


def index_orders(path):
    idx, rows, bytes_, t0 = {}, 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b)
        df["o_orderdate"] = pd.to_datetime(df["o_orderdate"])
        bytes_ += df["o_orderdate"].memory_usage(deep=True)
        for d in df["o_orderdate"].unique():
            idx.setdefault(d, BitMap()).update(i + rows for i in df.index[df["o_orderdate"] == d])
        rows += n
    return idx, rows, bytes_, time.perf_counter() - t0


def index_line(path):
    bm_commit_lt_receipt = BitMap()
    rows, bytes_, t0 = 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b)
        df["l_commitdate"]  = pd.to_datetime(df["l_commitdate"])
        df["l_receiptdate"] = pd.to_datetime(df["l_receiptdate"])
        bytes_ += df["l_commitdate"].memory_usage(deep=True)
        bytes_ += df["l_receiptdate"].memory_usage(deep=True)

        loc = df.index[df["l_commitdate"] < df["l_receiptdate"]].tolist()
        bm_commit_lt_receipt.update(i + rows for i in loc)

        rows += n
    return bm_commit_lt_receipt, rows, bytes_, time.perf_counter() - t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)
    want, cur, off, out = iter(sorted(bitmap)), None, 0, []
    cur = next(want, None)
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n, loc = len(b), []
        while cur is not None and cur < off + n:
            loc.append(cur - off); cur = next(want, None)
        if loc: out.append(b.to_pandas()[cols].iloc[loc])
        off += n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def write_filtered(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prepare_duckdb(o_df, l_df, sql_file):
    dest_o = write_filtered(o_df, "../data/tpch/parquet/filtered_orders.parquet")
    dest_l = write_filtered(l_df, "../data/tpch/parquet/filtered_lineitem.parquet")
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{dest_o}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest_l}')")
    return con, open(sql_file).read()


def prepare_datafusion(o_df, l_df, sql_file):
    dest_o = write_filtered(o_df, "../data/tpch/parquet/filtered_orders.parquet")
    dest_l = write_filtered(l_df, "../data/tpch/parquet/filtered_lineitem.parquet")
    ctx = SessionContext()
    ctx.register_parquet("orders", dest_o)
    ctx.register_parquet("lineitem", dest_l)
    return ctx, open(sql_file).read()


if __name__ == "__main__":
    PATH_O = "../data/tpch/parquet/orders.parquet"
    PATH_L = "../data/tpch/parquet/lineitem.parquet"

    date_idx, rows_o, bytes_o, sec_o = index_orders(PATH_O)
    commit_lt_bm, rows_l, bytes_l, sec_l = index_line(PATH_L)

    bm_orders = measure_query_execution(lambda:
        BitMap(range(rows_o)) & BitMap().union(
            *[bm for d, bm in date_idx.items() if DATE_FROM <= d < DATE_TO]
        )
    )
    bm_lines = measure_query_execution(lambda:
        BitMap(range(rows_l)) & commit_lt_bm
    )

    df_o = materialise(PATH_O, bm_orders["result"],
                       ["o_orderkey", "o_orderpriority", "o_orderdate"])
    df_l = materialise(PATH_L, bm_lines["result"],
                       ["l_orderkey", "l_commitdate", "l_receiptdate"])

    bitmap_mb   = bitmap_memory_size(date_idx, {"lt_commit": commit_lt_bm})
    original_mb = (bytes_o + bytes_l) / (1024 * 1024)
    build_secs  = sec_o + sec_l

    con, q_duck = prepare_duckdb(df_o, df_l, "../data/tpch/queries/4.sql")
    eng_duck    = measure_query_duckdb(4, con, q_duck)
    res_duck    = aggregate_metrics(bm_orders, bm_lines, eng_duck)     

    res_duck.update({
        "Query": 4,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_duck.pop("result", None); res_duck.pop("error", None)

    ctx, q_df  = prepare_datafusion(df_o, df_l, "../data/tpch/queries/4.sql")
    eng_df     = measure_query_datafusion(4, ctx, q_df)
    res_df     = aggregate_metrics(bm_orders, bm_lines, eng_df)        
    res_df.update({
        "Query": 4,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_df.pop("result", None); res_df.pop("error", None)

    HEAD = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)",
    ]
    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, HEAD, [res_duck])
    write_csv_results(out_df,   HEAD, [res_df])
