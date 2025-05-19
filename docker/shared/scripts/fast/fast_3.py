import os
import sys
import time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
import cppyy
from datafusion import SessionContext
from common_fast_tree import (
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
    aggregate_metrics,
)
from common import measure_query_execution
from datetime import datetime

FILE_LINEITEM = "../data/tpch/parquet/lineitem.parquet"
FILE_ORDERS   = "../data/tpch/parquet/orders.parquet"
FILE_CUSTOMER = "../data/tpch/parquet/customer.parquet"

BATCH       = 100_000
CUTOFF_DATE = "1995-03-15"
QUERY_PATH  = "../data/tpch/queries/3.sql"
RESULT_DIR = "../results/fast"

FIELDNAMES = [
    "Query",
    "Latency (s)",
    "CPU Usage (%)",
    "Peak Memory Usage (MB)",
    "Average Memory Usage (MB)",
    "IOPS (ops/s)",
    "Fast Tree Size (MB)",
    "Original Column Size (MB)",
    "Fast Tree Creation Time (s)",
]

def date_to_int32(date_str: str) -> int:
    dt    = datetime.strptime(date_str, "%Y-%m-%d")
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).days)

def build_date_fast_tree(file_path: str, batch_size: int, cutoff_date: str):
    FastTree = cppyy.gbl.fast.FastTree[3]
    Entry    = FastTree.Entry['int32_t', 'uint64_t']
    cpp_entries = cppyy.gbl.std.vector[Entry]()
    
    total = pq.ParquetFile(file_path).metadata.num_rows
    cpp_entries.reserve(total)
    
    cutoff_int    = date_to_int32(cutoff_date)
    orig_bytes    = 0
    global_offset = 0
    
    t0 = time.perf_counter()
    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        if "l_shipdate" in df.columns:
            col = df["l_shipdate"].dropna()
            orig_bytes += col.memory_usage(deep=True)
            for local_idx, ship_date in col.items():
                date_int = date_to_int32(ship_date.strftime("%Y-%m-%d"))
                row_idx  = global_offset + local_idx
                cpp_entries.emplace_back(int(date_int), int(row_idx))
        global_offset += len(df)
    fast_tree = FastTree()
    fast_tree.build(cpp_entries)
    build_secs = time.perf_counter() - t0
    return fast_tree, cpp_entries, orig_bytes, build_secs, cutoff_int

def materialize_filtered_indices(file_path: str, indices: set, batch_size: int) -> pd.DataFrame:
    if not indices:
        return pd.DataFrame()
    wanted     = sorted(indices)
    wanted_set = set(wanted)
    global_offset = 0
    out_frames = []
    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        n_rows = len(batch)
        df     = batch.to_pandas()
        local_ix = [i for i in range(n_rows) if (i + global_offset) in wanted_set]
        if local_ix:
            out_frames.append(df.iloc[local_ix])
        global_offset += n_rows
    return pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame()

def _cast_numeric(df: pd.DataFrame):
    for col in ("l_extendedprice", "l_quantity", "l_discount", "l_tax"):
        if col in df.columns:
            df[col] = df[col].astype("float64")

def prepare_duckdb(filtered_li: pd.DataFrame, query_file: str):
    _cast_numeric(filtered_li)
    dest_li = "../data/tpch/parquet/filtered_lineitem.parquet"
    filtered_li.to_parquet(dest_li, index=False, engine="pyarrow")
    
    con = duckdb.connect(":memory:")
    con.execute(f"""
        CREATE TABLE lineitem AS
          SELECT * FROM read_parquet('{dest_li}')
    """)
    con.execute(f"""
        CREATE TABLE orders AS
          SELECT * FROM read_parquet('{FILE_ORDERS}')
    """)
    con.execute(f"""
        CREATE TABLE customer AS
          SELECT * FROM read_parquet('{FILE_CUSTOMER}')
    """)
    sql = open(query_file).read()
    return con, sql

def prepare_datafusion(filtered_li: pd.DataFrame, query_file: str):
    _cast_numeric(filtered_li)
    dest_li = "../data/tpch/parquet/filtered_lineitem.parquet"
    filtered_li.to_parquet(dest_li, index=False, engine="pyarrow")
    
    ctx = SessionContext()
    ctx.register_parquet("lineitem",   dest_li)
    ctx.register_parquet("orders",     FILE_ORDERS)
    ctx.register_parquet("customer",   FILE_CUSTOMER)
    
    sql = open(query_file).read()
    return ctx, sql

if __name__ == "__main__":
    os.makedirs(os.path.join(RESULT_DIR, "duckdb"),   exist_ok=True)
    os.makedirs(os.path.join(RESULT_DIR, "datafusion"), exist_ok=True)
    
    cppyy.add_include_path(".")
    cppyy.load_library("./fast/lib/libfast.so")
    cppyy.include("./fast/src/fast.hpp")

    fast_tree, cpp_entries, orig_bytes, build_secs, cutoff_int = build_date_fast_tree(
        FILE_LINEITEM, BATCH, CUTOFF_DATE
    )

    lookup_metrics = measure_query_execution(
        lambda: fast_tree.rangeGreaterThan(cutoff_int, cpp_entries).entries
    )
    cpp_result_entries  = lookup_metrics["result"]
    filtered_indices    = set(int(e.value) for e in cpp_result_entries)

    filtered_df = materialize_filtered_indices(
        FILE_LINEITEM, filtered_indices, BATCH
    )

    fast_tree_mb   = fast_tree.getMemoryUsage() / (1024 * 1024)
    original_mb    = orig_bytes    / (1024 * 1024)

    con, sql_q3 = prepare_duckdb(filtered_df, QUERY_PATH)
    engine_metrics_duck = measure_query_duckdb(3, con, sql_q3)
    combined_duck = aggregate_metrics(lookup_metrics, engine_metrics_duck)
    combined_duck.update({
        "Query": 3,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_duck = os.path.join(RESULT_DIR, "duckdb", "fast_tpch.csv")
    write_csv_results(out_duck, FIELDNAMES, [combined_duck])

    ctx, sql_q3_df = prepare_datafusion(filtered_df, QUERY_PATH)
    engine_metrics_df = measure_query_datafusion(3, ctx, sql_q3_df)
    combined_df = aggregate_metrics(lookup_metrics, engine_metrics_df)
    combined_df.update({
        "Query": 3,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_df = os.path.join(RESULT_DIR, "datafusion", "fast_tpch.csv")
    write_csv_results(out_df, FIELDNAMES, [combined_df])
