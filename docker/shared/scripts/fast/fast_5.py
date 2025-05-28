import os
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

FILE_ORDERS   = "../data/tpch/parquet/orders.parquet"
FILE_LINEITEM = "../data/tpch/parquet/lineitem.parquet"
FILE_CUSTOMER = "../data/tpch/parquet/customer.parquet"
FILE_SUPPLIER = "../data/tpch/parquet/supplier.parquet"
FILE_NATION   = "../data/tpch/parquet/nation.parquet"
FILE_REGION   = "../data/tpch/parquet/region.parquet"

BATCH       = 6000000
START_DATE  = "1994-01-01"
END_DATE    = "1995-01-01"  
QUERY_PATH  = "../data/tpch/queries/5.sql"
RESULT_DIR  = "../results/fast/"

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

def build_date_fast_tree(file_path: str, batch_size: int, date_col: str):
    FastTree = cppyy.gbl.fast.FastTree[3]
    Entry    = FastTree.Entry['int32_t', 'uint64_t']
    cpp_entries = cppyy.gbl.std.vector[Entry]()

    total = pq.ParquetFile(file_path).metadata.num_rows
    cpp_entries.reserve(total)

    orig_bytes    = 0
    global_offset = 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        if date_col in df.columns:
            col = df[date_col].dropna()
            orig_bytes += col.memory_usage(deep=True)
            for local_idx, dt_val in col.items():
                date_int = date_to_int32(dt_val.strftime("%Y-%m-%d"))
                cpp_entries.emplace_back(date_int, global_offset + local_idx)
        global_offset += len(df)

    fast_tree = FastTree()
    fast_tree.build(cpp_entries)
    build_secs = time.perf_counter() - t0

    return fast_tree, cpp_entries, orig_bytes, build_secs

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

def prepare_duckdb(filtered_orders: pd.DataFrame, query_file: str):
    dest_ord = "../data/tpch/parquet/filtered_orders.parquet"
    filtered_orders.to_parquet(dest_ord, index=False, engine="pyarrow")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE orders    AS SELECT * FROM read_parquet('{dest_ord}')")
    con.execute(f"CREATE TABLE lineitem  AS SELECT * FROM read_parquet('{FILE_LINEITEM}')")
    con.execute(f"CREATE TABLE customer  AS SELECT * FROM read_parquet('{FILE_CUSTOMER}')")
    con.execute(f"CREATE TABLE supplier  AS SELECT * FROM read_parquet('{FILE_SUPPLIER}')")
    con.execute(f"CREATE TABLE nation    AS SELECT * FROM read_parquet('{FILE_NATION}')")
    con.execute(f"CREATE TABLE region    AS SELECT * FROM read_parquet('{FILE_REGION}')")

    sql = open(query_file).read()
    return con, sql

def prepare_datafusion(filtered_orders: pd.DataFrame, query_file: str):
    dest_ord = "../data/tpch/parquet/filtered_orders.parquet"
    filtered_orders.to_parquet(dest_ord, index=False, engine="pyarrow")

    ctx = SessionContext()
    ctx.register_parquet("orders",    dest_ord)
    ctx.register_parquet("lineitem",  FILE_LINEITEM)
    ctx.register_parquet("customer",  FILE_CUSTOMER)
    ctx.register_parquet("supplier",  FILE_SUPPLIER)
    ctx.register_parquet("nation",    FILE_NATION)
    ctx.register_parquet("region",    FILE_REGION)

    sql = open(query_file).read()
    return ctx, sql

if __name__ == "__main__":
    os.makedirs(os.path.join(RESULT_DIR, "duckdb"),    exist_ok=True)
    os.makedirs(os.path.join(RESULT_DIR, "datafusion"), exist_ok=True)

    cppyy.add_include_path(".")
    cppyy.load_library("./fast/lib/libfast.so")
    cppyy.include("./fast/src/fast.hpp")

    fast_tree, cpp_entries, orig_bytes, build_secs = build_date_fast_tree(
        FILE_ORDERS, BATCH, "o_orderdate"
    )
    fast_tree_mb = fast_tree.getMemoryUsage() / (1024 * 1024)
    original_mb  = orig_bytes / (1024 * 1024)

    start_int = date_to_int32(START_DATE)
    end_int   = date_to_int32(END_DATE) - 1

    lookup_metrics = measure_query_execution(
        lambda: fast_tree.rangeSearch(start_int, end_int, cpp_entries).entries
    )
    cpp_result_entries = lookup_metrics["result"]
    filtered_indices   = set(int(e.value) for e in cpp_result_entries)

    filtered_orders = materialize_filtered_indices(
        FILE_ORDERS, filtered_indices, BATCH
    )

    con, sql_q5 = prepare_duckdb(filtered_orders, QUERY_PATH)
    engine_metrics_duck = measure_query_duckdb(5, con, sql_q5)
    combined_duck = aggregate_metrics(lookup_metrics, engine_metrics_duck)
    combined_duck.update({
        "Query": 5,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_duck = os.path.join(RESULT_DIR, "duckdb", "fast_tpch.csv")
    write_csv_results(out_duck, FIELDNAMES, [combined_duck])

    ctx, sql_q5_df = prepare_datafusion(filtered_orders, QUERY_PATH)
    engine_metrics_df = measure_query_datafusion(5, ctx, sql_q5_df)
    combined_df = aggregate_metrics(lookup_metrics, engine_metrics_df)
    combined_df.update({
        "Query": 5,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_df = os.path.join(RESULT_DIR, "datafusion", "fast_tpch.csv")
    write_csv_results(out_df, FIELDNAMES, [combined_df])
