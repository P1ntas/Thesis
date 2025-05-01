import os, sys, time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from functools import reduce
from pyroaring import BitMap
from datafusion import SessionContext

from common_roaring import (
    bitmap_memory_size,
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
)
from common import measure_query_execution                   

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.makedirs("../results", exist_ok=True)

BATCH = 100_000
SHIPMODES = {"MAIL", "SHIP"}
R_FROM = pd.to_datetime("1994-01-01")
R_TO   = pd.to_datetime("1995-01-01")         
NUM_RUNS_SQL = 3


def or_reduce(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_orders(path: str):
    idx_okey, rows, bytes_ = {}, 0, 0
    t0 = time.perf_counter()
    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        df = batch.to_pandas(); n=len(df)
        bytes_ += df["o_orderkey"].memory_usage(deep=True)
        for key, loc in zip(df["o_orderkey"], df.index):
            idx_okey.setdefault(key, BitMap()).add(loc + rows)
        rows += n
        del df
    return idx_okey, rows, bytes_, time.perf_counter() - t0


def index_line(path: str):
    idx_shipmode = {}
    bm_commit_lt_receipt = BitMap()
    bm_ship_lt_commit   = BitMap()
    bm_receipt_range    = BitMap()

    rows, bytes_ = 0, 0
    t0 = time.perf_counter()
    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        df = batch.to_pandas(); n=len(df)

        for col in ("l_commitdate", "l_receiptdate", "l_shipdate"):
            df[col] = pd.to_datetime(df[col])
            bytes_ += df[col].memory_usage(deep=True)

        bytes_ += df["l_shipmode"].memory_usage(deep=True)

        for v in df["l_shipmode"].unique():
            idx_shipmode.setdefault(v, BitMap()).update(i + rows
                                                        for i in df.index[df["l_shipmode"] == v])
        commit_lt_mask = df["l_commitdate"] < df["l_receiptdate"]
        bm_commit_lt_receipt.update((df.index[commit_lt_mask] + rows).tolist())

        ship_lt_mask = df["l_shipdate"] < df["l_commitdate"]
        bm_ship_lt_commit.update((df.index[ship_lt_mask] + rows).tolist())

        range_mask = (df["l_receiptdate"] >= R_FROM) & (df["l_receiptdate"] < R_TO)
        bm_receipt_range.update((df.index[range_mask] + rows).tolist())

        rows += n
        del df
    secs = time.perf_counter() - t0
    return (idx_shipmode, bm_commit_lt_receipt, bm_ship_lt_commit,
            bm_receipt_range, rows, bytes_, secs)


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want = iter(sorted(bitmap)); cur = next(want, None)
    out, off = [], 0
    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        n=len(batch); loc=[]
        while cur is not None and cur < off + n:
            loc.append(cur - off)
            cur = next(want, None)
        if loc:
            out.append(batch.to_pandas()[cols].iloc[loc])
        off += n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def save_parquet(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prep_duck(o_df, l_df, sql_file):
    dest_o = save_parquet(o_df, "../data/tpch/parquet/filtered_orders.parquet")
    dest_l = save_parquet(l_df, "../data/tpch/parquet/filtered_lineitem.parquet")
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{dest_o}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest_l}')")
    return con, open(sql_file).read()


def prep_df(o_df, l_df, sql_file):
    dest_o = save_parquet(o_df, "../data/tpch/parquet/filtered_orders.parquet")
    dest_l = save_parquet(l_df, "../data/tpch/parquet/filtered_lineitem.parquet")
    ctx = SessionContext()
    ctx.register_parquet("orders", dest_o)
    ctx.register_parquet("lineitem", dest_l)
    return ctx, open(sql_file).read()


if __name__ == "__main__":

    PATH_O = "../data/tpch/parquet/orders.parquet"
    PATH_L = "../data/tpch/parquet/lineitem.parquet"

    idx_okey, rows_o, bytes_o, sec_o = index_orders(PATH_O)
    (idx_ship, bm_clt, bm_slt, bm_range,
     rows_l, bytes_l, sec_l) = index_line(PATH_L)

    shipmode_bm = or_reduce([idx_ship[m] for m in SHIPMODES if m in idx_ship])

    bm_line = measure_query_execution(
        lambda: shipmode_bm & bm_clt & bm_slt & bm_range
    )
    line_bitmap = bm_line["result"]

    surviving_keys = {k for k, bm in idx_okey.items() if bm & line_bitmap}
    bm_orders = measure_query_execution(
        lambda: or_reduce([idx_okey[k] for k in surviving_keys])
    )
    orders_bitmap = bm_orders["result"]

    df_o = materialise(PATH_O, orders_bitmap,
                       ["o_orderkey","o_orderpriority"])
    df_l = materialise(PATH_L, line_bitmap,
                       ["l_orderkey","l_shipmode",
                        "l_commitdate","l_receiptdate","l_shipdate"])

    bitmap_mb   = bitmap_memory_size(idx_okey, idx_ship,
                                     {"cmp": bm_clt}, {"slt": bm_slt}, {"rng": bm_range})
    original_mb = (bytes_o + bytes_l) / (1024*1024)
    build_secs  = sec_o + sec_l

    SQL = "../data/tpch/queries/12.sql"
    con, q_duck = prep_duck(df_o, df_l, SQL)
    res_duck = measure_query_duckdb(12, con, q_duck, num_runs=NUM_RUNS_SQL)

    ctx, q_df = prep_df(df_o, df_l, SQL)
    res_df  = measure_query_datafusion(12, ctx, q_df, num_runs=NUM_RUNS_SQL)

    for tgt in (res_duck, res_df):
        for k in ("Latency (s)", "IOPS (ops/s)"):
            tgt[k] += (bm_orders.get(k,0) or 0) + (bm_line.get(k,0) or 0)
        for k in ("Peak Memory Usage (MB)", "Average Memory Usage (MB)"):
            tgt[k] = max(tgt[k], bm_orders.get(k,0) or 0, bm_line.get(k,0) or 0)

        tgt.update({
            "Roaring Bitmap Size (MB)": bitmap_mb,
            "Original Columns Size (MB)": original_mb,
            "Bitmap Creation Time (s)":  build_secs,
        })
        for junk in ("result", "error"):
            tgt.pop(junk, None)

    HEADERS = [
        "Query","Latency (s)","CPU Usage (%)",
        "Peak Memory Usage (MB)","Average Memory Usage (MB)","IOPS (ops/s)",
        "Roaring Bitmap Size (MB)","Original Columns Size (MB)","Bitmap Creation Time (s)"
    ]
    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, HEADERS, [res_duck])
    write_csv_results(out_df,   HEADERS, [res_df])
