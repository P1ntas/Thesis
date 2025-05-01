import os, sys, time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from pyroaring import BitMap
from datafusion import SessionContext
from functools import reduce

from common_roaring import (
    bitmap_memory_size,
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
)
from common import measure_query_execution        

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.makedirs("../results", exist_ok=True)

BATCH        = 100_000
NUM_RUNS_SQL = 3                     

DATE_FROM = pd.to_datetime("1993-10-01")
DATE_TO   = pd.to_datetime("1994-01-01")           
RET_FLAG  = "R"


def combine(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def idx_orders(path: str):
    idx_date, rows, bytes_ = {}, 0, 0
    t0 = time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df = b.to_pandas()
        df["o_orderdate"] = pd.to_datetime(df["o_orderdate"])
        n = len(df)
        bytes_ += df["o_orderdate"].memory_usage(deep=True)
        for d in df["o_orderdate"].unique():
            idx_date.setdefault(d, BitMap()).update(i + rows
                                                    for i in df.index[df["o_orderdate"] == d])
        rows += n
        del df
    return idx_date, rows, bytes_, time.perf_counter() - t0


def idx_line(path: str):
    idx_retflag, rows, bytes_ = {}, 0, 0
    t0 = time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df = b.to_pandas(); n=len(df)
        bytes_ += df["l_returnflag"].memory_usage(deep=True)
        for v in df["l_returnflag"].unique():
            idx_retflag.setdefault(v, BitMap()).update(i + rows
                                                       for i in df.index[df["l_returnflag"] == v])
        rows += n
        del df
    return idx_retflag, rows, bytes_, time.perf_counter() - t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)
    want=iter(sorted(bitmap)); cur=next(want,None)
    out=[]; off=0
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n=len(b); loc=[]
        while cur is not None and cur < off+n:
            loc.append(cur-off)
            cur=next(want,None)
        if loc:
            out.append(b.to_pandas()[cols].iloc[loc])
        off+=n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def write_filtered(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prep_duck(customer, orders, line, nation, sql_file):
    for col in ("l_extendedprice", "l_discount"):
        if col in line.columns:
            line[col] = line[col].astype("float64")
    dc = write_filtered(customer,'../data/tpch/parquet/filtered_customer.parquet')
    do = write_filtered(orders,  '../data/tpch/parquet/filtered_orders.parquet')
    dl = write_filtered(line,    '../data/tpch/parquet/filtered_lineitem.parquet')
    dn = write_filtered(nation,  '../data/tpch/parquet/filtered_nation.parquet')
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{dc}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{do}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dl}')")
    con.execute(f"CREATE TABLE nation   AS SELECT * FROM read_parquet('{dn}')")
    return con, open(sql_file).read()


def prep_datafusion(customer, orders, line, nation, sql_file):
    for col in ("l_extendedprice", "l_discount"):
        if col in line.columns:
            line[col] = line[col].astype("float64")
    dc = write_filtered(customer,'../data/tpch/parquet/filtered_customer.parquet')
    do = write_filtered(orders,  '../data/tpch/parquet/filtered_orders.parquet')
    dl = write_filtered(line,    '../data/tpch/parquet/filtered_lineitem.parquet')
    dn = write_filtered(nation,  '../data/tpch/parquet/filtered_nation.parquet')
    ctx = SessionContext()
    ctx.register_parquet("customer", dc)
    ctx.register_parquet("orders",   do)
    ctx.register_parquet("lineitem", dl)
    ctx.register_parquet("nation",   dn)
    return ctx, open(sql_file).read()


if __name__ == "__main__":

    PATH_C = "../data/tpch/parquet/customer.parquet"
    PATH_O = "../data/tpch/parquet/orders.parquet"
    PATH_L = "../data/tpch/parquet/lineitem.parquet"
    PATH_N = "../data/tpch/parquet/nation.parquet"

    o_idx, rows_o, bytes_o, sec_o = idx_orders(PATH_O)
    r_idx, rows_l, bytes_l, sec_l = idx_line(PATH_L)

    date_bm = combine([bm for d,bm in o_idx.items() if DATE_FROM <= d < DATE_TO])
    bm_orders = measure_query_execution(lambda: BitMap(range(rows_o)) & date_bm)
    orders_bitmap = bm_orders["result"]

    bm_lines = measure_query_execution(
        lambda: BitMap(range(rows_l)) & r_idx.get(RET_FLAG, BitMap())
    )
    lines_bitmap = bm_lines["result"]

    df_o = materialise(PATH_O, orders_bitmap,
                       ["o_orderkey","o_custkey","o_orderdate"])
    df_l = materialise(PATH_L, lines_bitmap,
                       ["l_orderkey","l_extendedprice","l_discount","l_returnflag"])
    df_c = pd.read_parquet(PATH_C)
    df_n = pd.read_parquet(PATH_N)

    bitmap_mb   = bitmap_memory_size(o_idx, r_idx)
    original_mb = (bytes_o + bytes_l) / (1024*1024)
    build_secs  = sec_o + sec_l

    SQL_FILE = "../data/tpch/queries/10.sql"
    con, q_duck = prep_duck(df_c, df_o, df_l, df_n, SQL_FILE)
    res_duck    = measure_query_duckdb(10, con, q_duck, num_runs=NUM_RUNS_SQL)

    ctx, q_df   = prep_datafusion(df_c, df_o, df_l, df_n, SQL_FILE)
    res_df      = measure_query_datafusion(10, ctx, q_df, num_runs=NUM_RUNS_SQL)

    for tgt in (res_duck, res_df):
        for k in ("Latency (s)", "IOPS (ops/s)"):
            tgt[k] += (bm_orders.get(k,0) or 0) + (bm_lines.get(k,0) or 0)
        for k in ("Peak Memory Usage (MB)", "Average Memory Usage (MB)"):
            tgt[k] = max(tgt[k], bm_orders.get(k,0) or 0, bm_lines.get(k,0) or 0)

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
        "Roaring Bitmap Size (MB)","Original Columns Size (MB)","Bitmap Creation Time (s)",
    ]
    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, HEADERS, [res_duck])
    write_csv_results(out_df,   HEADERS, [res_df])
