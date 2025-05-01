import os, sys, time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
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

NUM_RUNS = 3
BATCH    = 100_000

REGION   = "ASIA"
FROM     = pd.to_datetime("1994-01-01")
TO       = pd.to_datetime("1995-01-01")        


def index_region(path: str):
    idx_name = {}
    rows, bytes_ = 0, 0
    t0 = time.perf_counter()

    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df = b.to_pandas()
        bytes_ += df["r_name"].memory_usage(deep=True)
        n = len(df)

        for v in df["r_name"].unique():
            idx_name.setdefault(v, BitMap()).update(i + rows
                                                    for i in df.index[df["r_name"] == v])
        rows += n
        del df

    return idx_name, rows, bytes_, time.perf_counter() - t0


def index_orders(path: str):
    idx_date = {}
    rows, bytes_ = 0, 0
    t0 = time.perf_counter()

    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df = b.to_pandas()
        df["o_orderdate"] = pd.to_datetime(df["o_orderdate"])
        bytes_ += df["o_orderdate"].memory_usage(deep=True)
        n = len(df)

        for d in df["o_orderdate"].unique():
            idx_date.setdefault(d, BitMap()).update(i + rows
                                                    for i in df.index[df["o_orderdate"] == d])
        rows += n
        del df

    return idx_date, rows, bytes_, time.perf_counter() - t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want = iter(sorted(bitmap))
    cur  = next(want, None)
    out, offset = [], 0

    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n = len(b); loc = []
        while cur is not None and cur < offset + n:
            loc.append(cur - offset)
            cur = next(want, None)
        if loc:
            out.append(b.to_pandas()[cols].iloc[loc])
        offset += n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def write_filtered(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prepare_duckdb(region, nation, cust, orde, supp, line, sql_file):
    for col in ("l_extendedprice", "l_discount"):
        if col in line.columns:
            line[col] = line[col].astype("float64")

    dest_r = write_filtered(region, "../data/tpch/parquet/filtered_region.parquet")
    dest_n = write_filtered(nation,  "../data/tpch/parquet/filtered_nation.parquet")
    dest_c = write_filtered(cust,    "../data/tpch/parquet/filtered_customer.parquet")
    dest_o = write_filtered(orde,    "../data/tpch/parquet/filtered_orders.parquet")
    dest_s = write_filtered(supp,    "../data/tpch/parquet/filtered_supplier.parquet")
    dest_l = write_filtered(line,    "../data/tpch/parquet/filtered_lineitem.parquet")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE region   AS SELECT * FROM read_parquet('{dest_r}')")
    con.execute(f"CREATE TABLE nation   AS SELECT * FROM read_parquet('{dest_n}')")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{dest_c}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{dest_o}')")
    con.execute(f"CREATE TABLE supplier AS SELECT * FROM read_parquet('{dest_s}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest_l}')")
    return con, open(sql_file).read()


def prepare_datafusion(region, nation, cust, orde, supp, line, sql_file):
    for col in ("l_extendedprice", "l_discount"):
        if col in line.columns:
            line[col] = line[col].astype("float64")

    dest_r = write_filtered(region, "../data/tpch/parquet/filtered_region.parquet")
    dest_n = write_filtered(nation,  "../data/tpch/parquet/filtered_nation.parquet")
    dest_c = write_filtered(cust,    "../data/tpch/parquet/filtered_customer.parquet")
    dest_o = write_filtered(orde,    "../data/tpch/parquet/filtered_orders.parquet")
    dest_s = write_filtered(supp,    "../data/tpch/parquet/filtered_supplier.parquet")
    dest_l = write_filtered(line,    "../data/tpch/parquet/filtered_lineitem.parquet")

    ctx = SessionContext()
    ctx.register_parquet("region",   dest_r)
    ctx.register_parquet("nation",   dest_n)
    ctx.register_parquet("customer", dest_c)
    ctx.register_parquet("orders",   dest_o)
    ctx.register_parquet("supplier", dest_s)
    ctx.register_parquet("lineitem", dest_l)
    return ctx, open(sql_file).read()


if __name__ == "__main__":

    PATH_R = "../data/tpch/parquet/region.parquet"
    PATH_N = "../data/tpch/parquet/nation.parquet"
    PATH_C = "../data/tpch/parquet/customer.parquet"
    PATH_O = "../data/tpch/parquet/orders.parquet"
    PATH_S = "../data/tpch/parquet/supplier.parquet"
    PATH_L = "../data/tpch/parquet/lineitem.parquet"

    r_idx, rows_r, bytes_r, sec_r = index_region(PATH_R)
    o_idx, rows_o, bytes_o, sec_o = index_orders(PATH_O)

    bm_region = measure_query_execution(
        lambda: BitMap(range(rows_r)) & r_idx.get(REGION, BitMap())
    )
    region_bitmap = bm_region["result"]

    date_bm = BitMap()
    for d, bm in o_idx.items():
        if FROM <= d < TO:
            date_bm |= bm

    bm_orders = measure_query_execution(
        lambda: BitMap(range(rows_o)) & date_bm
    )
    orders_bitmap = bm_orders["result"]

    df_r = materialise(PATH_R, region_bitmap,   ["r_regionkey", "r_name"])
    df_n = pd.read_parquet(PATH_N)            
    df_c = pd.read_parquet(PATH_C)           
    df_o = materialise(PATH_O, orders_bitmap,
                       ["o_orderkey", "o_custkey", "o_orderdate"])
    df_s = pd.read_parquet(PATH_S)        
    df_l = pd.read_parquet(PATH_L)

    bitmap_mb   = bitmap_memory_size(r_idx, o_idx)
    original_mb = (bytes_r + bytes_o) / (1024 * 1024)
    build_secs  = sec_r + sec_o

    SQL_FILE = "../data/tpch/queries/5.sql"
    con, sql_duck = prepare_duckdb(df_r, df_n, df_c, df_o, df_s, df_l, SQL_FILE)
    res_duck = measure_query_duckdb(5, con, sql_duck, num_runs=3)

    ctx, sql_df = prepare_datafusion(df_r, df_n, df_c, df_o, df_s, df_l, SQL_FILE)
    res_df   = measure_query_datafusion(5, ctx, sql_df, num_runs=3)

    total_build_latency = (bm_region["Latency (s)"] or 0) + (bm_orders["Latency (s)"] or 0)

    for tgt in (res_duck, res_df):
        tgt["Latency (s)"] += total_build_latency

        tgt["Peak Memory Usage (MB)"]    = max(
            tgt["Peak Memory Usage (MB)"],
            bm_region["Peak Memory Usage (MB)"],
            bm_orders["Peak Memory Usage (MB)"]
        )
        tgt["Average Memory Usage (MB)"] = max(
            tgt["Average Memory Usage (MB)"],
            bm_region["Average Memory Usage (MB)"],
            bm_orders["Average Memory Usage (MB)"]
        )
        tgt["IOPS (ops/s)"]              = max(
            tgt["IOPS (ops/s)"],
            bm_region["IOPS (ops/s)"],
            bm_orders["IOPS (ops/s)"]
        )

        tgt.update({
            "Roaring Bitmap Size (MB)":      bitmap_mb,
            "Original Columns Size (MB)":    original_mb,
            "Bitmap Creation Time (s)":      build_secs,
        })

        for junk in ("result", "error"):
            tgt.pop(junk, None)

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
