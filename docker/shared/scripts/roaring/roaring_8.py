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

REGION_NAME  = "AMERICA"
PART_TYPE    = "ECONOMY ANODIZED STEEL"
FROM         = pd.to_datetime("1995-01-01")
TO           = pd.to_datetime("1997-01-01")      


def combine(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def idx_region(path):
    idx_name, rows, bytes_ = {}, 0, 0
    t0 = time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df = b.to_pandas(); n=len(df)
        bytes_ += df["r_name"].memory_usage(deep=True)
        for v in df["r_name"].unique():
            idx_name.setdefault(v, BitMap()).update(i+rows
                                                    for i in df.index[df["r_name"]==v])
        rows += n; del df
    return idx_name, rows, bytes_, time.perf_counter()-t0


def idx_part(path):
    idx_type, rows, bytes_ = {}, 0, 0
    t0 = time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df=b.to_pandas(); n=len(df)
        bytes_+=df["p_type"].memory_usage(deep=True)
        for v in df["p_type"].unique():
            idx_type.setdefault(v, BitMap()).update(i+rows
                                                    for i in df.index[df["p_type"]==v])
        rows+=n; del df
    return idx_type, rows, bytes_, time.perf_counter()-t0


def idx_orders(path):
    idx_date, rows, bytes_ = {}, 0, 0
    t0 = time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df=b.to_pandas(); df["o_orderdate"]=pd.to_datetime(df["o_orderdate"]); n=len(df)
        bytes_+=df["o_orderdate"].memory_usage(deep=True)
        for d in df["o_orderdate"].unique():
            idx_date.setdefault(d, BitMap()).update(i+rows
                                                    for i in df.index[df["o_orderdate"]==d])
        rows+=n; del df
    return idx_date, rows, bytes_, time.perf_counter()-t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)
    want=iter(sorted(bitmap)); cur=next(want,None)
    out=[]; off=0
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n=len(b); loc=[]
        while cur is not None and cur < off+n:
            loc.append(cur-off); cur=next(want,None)
        if loc: out.append(b.to_pandas()[cols].iloc[loc])
        off+=n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def write_filtered(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prepare_duckdb(r, n, p, s, l, o, c, sql_file):
    for col in ("l_extendedprice", "l_discount"):
        if col in l.columns:
            l[col]=l[col].astype("float64")
    dest_r=write_filtered(r,'../data/tpch/parquet/filtered_region.parquet')
    dest_n=write_filtered(n,'../data/tpch/parquet/filtered_nation.parquet')
    dest_p=write_filtered(p,'../data/tpch/parquet/filtered_part.parquet')
    dest_s=write_filtered(s,'../data/tpch/parquet/filtered_supplier.parquet')
    dest_l=write_filtered(l,'../data/tpch/parquet/filtered_lineitem.parquet')
    dest_o=write_filtered(o,'../data/tpch/parquet/filtered_orders.parquet')
    dest_c=write_filtered(c,'../data/tpch/parquet/filtered_customer.parquet')
    con=duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE region   AS SELECT * FROM read_parquet('{dest_r}')")
    con.execute(f"CREATE TABLE nation   AS SELECT * FROM read_parquet('{dest_n}')")
    con.execute(f"CREATE TABLE part     AS SELECT * FROM read_parquet('{dest_p}')")
    con.execute(f"CREATE TABLE supplier AS SELECT * FROM read_parquet('{dest_s}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest_l}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{dest_o}')")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{dest_c}')")
    return con, open(sql_file).read()


def prepare_datafusion(r, n, p, s, l, o, c, sql_file):
    for col in ("l_extendedprice", "l_discount"):
        if col in l.columns:
            l[col]=l[col].astype("float64")
    dest_r=write_filtered(r,'../data/tpch/parquet/filtered_region.parquet')
    dest_n=write_filtered(n,'../data/tpch/parquet/filtered_nation.parquet')
    dest_p=write_filtered(p,'../data/tpch/parquet/filtered_part.parquet')
    dest_s=write_filtered(s,'../data/tpch/parquet/filtered_supplier.parquet')
    dest_l=write_filtered(l,'../data/tpch/parquet/filtered_lineitem.parquet')
    dest_o=write_filtered(o,'../data/tpch/parquet/filtered_orders.parquet')
    dest_c=write_filtered(c,'../data/tpch/parquet/filtered_customer.parquet')
    ctx=SessionContext()
    ctx.register_parquet("region",dest_r)
    ctx.register_parquet("nation",dest_n)
    ctx.register_parquet("part",dest_p)
    ctx.register_parquet("supplier",dest_s)
    ctx.register_parquet("lineitem",dest_l)
    ctx.register_parquet("orders",dest_o)
    ctx.register_parquet("customer",dest_c)
    return ctx, open(sql_file).read()


if __name__=="__main__":

    PATH_R='../data/tpch/parquet/region.parquet'
    PATH_N='../data/tpch/parquet/nation.parquet'
    PATH_P='../data/tpch/parquet/part.parquet'
    PATH_S='../data/tpch/parquet/supplier.parquet'
    PATH_L='../data/tpch/parquet/lineitem.parquet'
    PATH_O='../data/tpch/parquet/orders.parquet'
    PATH_C='../data/tpch/parquet/customer.parquet'

    r_idx, rows_r, bytes_r, sec_r = idx_region(PATH_R)
    p_idx, rows_p, bytes_p, sec_p = idx_part(PATH_P)
    o_idx, rows_o, bytes_o, sec_o = idx_orders(PATH_O)

    bm_r = measure_query_execution(
        lambda: BitMap(range(rows_r)) & r_idx.get(REGION_NAME, BitMap())
    )
    region_bitmap = bm_r["result"]

    bm_p = measure_query_execution(
        lambda: BitMap(range(rows_p)) & p_idx.get(PART_TYPE, BitMap())
    )
    part_bitmap = bm_p["result"]

    date_bm = combine([bm for d,bm in o_idx.items() if FROM <= d < TO])
    bm_o = measure_query_execution(
        lambda: BitMap(range(rows_o)) & date_bm
    )
    orders_bitmap = bm_o["result"]

    df_r = materialise(PATH_R, region_bitmap, ["r_regionkey","r_name"])
    df_p = materialise(PATH_P, part_bitmap,   ["p_partkey","p_type"])
    df_o = materialise(PATH_O, orders_bitmap,
                       ["o_orderkey","o_custkey","o_orderdate"])
    df_n = pd.read_parquet(PATH_N)
    df_s = pd.read_parquet(PATH_S)
    df_l = pd.read_parquet(PATH_L)
    df_c = pd.read_parquet(PATH_C)

    bitmap_mb   = bitmap_memory_size(r_idx, p_idx, o_idx)
    original_mb = (bytes_r + bytes_p + bytes_o) / (1024*1024)
    build_secs  = sec_r + sec_p + sec_o

    SQL_FILE = '../data/tpch/queries/8.sql'
    con, sql_duck = prepare_duckdb(df_r, df_n, df_p, df_s, df_l, df_o, df_c, SQL_FILE)
    res_duck      = measure_query_duckdb(8, con, sql_duck, num_runs=NUM_RUNS_SQL)

    ctx, sql_df   = prepare_datafusion(df_r, df_n, df_p, df_s, df_l, df_o, df_c, SQL_FILE)
    res_df        = measure_query_datafusion(8, ctx, sql_df, num_runs=NUM_RUNS_SQL)

    for tgt in (res_duck, res_df):
        for k in ("Latency (s)","IOPS (ops/s)"):
            tgt[k] += (bm_r.get(k,0) or 0) + (bm_p.get(k,0) or 0) + (bm_o.get(k,0) or 0)
        for k in ("Peak Memory Usage (MB)","Average Memory Usage (MB)"):
            tgt[k] = max(tgt[k],
                         bm_r.get(k,0) or 0,
                         bm_p.get(k,0) or 0,
                         bm_o.get(k,0) or 0)

        tgt.update({
            "Roaring Bitmap Size (MB)": bitmap_mb,
            "Original Columns Size (MB)": original_mb,
            "Bitmap Creation Time (s)":  build_secs,
        })
        for junk in ("result","error"):
            tgt.pop(junk,None)

    HEADERS=[
        "Query","Latency (s)","CPU Usage (%)",
        "Peak Memory Usage (MB)","Average Memory Usage (MB)","IOPS (ops/s)",
        "Roaring Bitmap Size (MB)","Original Columns Size (MB)",
        "Bitmap Creation Time (s)"
    ]
    out_duck="../results/roaring/duckdb/roaring_tpch.csv"
    out_df  ="../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck),exist_ok=True)
    os.makedirs(os.path.dirname(out_df),  exist_ok=True)

    write_csv_results(out_duck,HEADERS,[res_duck])
    write_csv_results(out_df,  HEADERS,[res_df])
