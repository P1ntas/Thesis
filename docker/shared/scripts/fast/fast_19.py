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

FILE_PART     = "../data/tpch/parquet/part.parquet"
FILE_LINEITEM = "../data/tpch/parquet/lineitem.parquet"

BATCH      = 6000000
SIZE_MIN   = 1
SIZE_MAX   = 15
QUERY_PATH = "../data/tpch/queries/19.sql"
RESULT_DIR = "../results/fast/"

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

def build_int_fast_tree(parquet_path: str, batch_size: int, col_name: str):
    FastTree = cppyy.gbl.fast.FastTree[3]
    Entry    = FastTree.Entry['int32_t', 'uint64_t']
    cpp_entries = cppyy.gbl.std.vector[Entry]()

    total = pq.ParquetFile(parquet_path).metadata.num_rows
    cpp_entries.reserve(total)

    orig_bytes    = 0
    global_offset = 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(parquet_path).iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        if col_name in df.columns:
            col = df[col_name].dropna().astype("int32")
            orig_bytes += col.memory_usage(deep=True)
            for local_idx, v in col.items():
                cpp_entries.emplace_back(int(v), global_offset + local_idx)
        global_offset += len(df)

    fast_tree = FastTree()
    fast_tree.build(cpp_entries)
    build_secs = time.perf_counter() - t0

    return fast_tree, cpp_entries, orig_bytes, build_secs

def materialize_filtered_indices(parquet_path: str, indices: set, batch_size: int) -> pd.DataFrame:
    if not indices:
        return pd.DataFrame()
    wanted     = sorted(indices)
    wanted_set = set(wanted)
    global_offset = 0
    frames = []
    pf = pq.ParquetFile(parquet_path)

    for batch in pf.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        n = len(df)
        picks = [i for i in range(n) if (i + global_offset) in wanted_set]
        if picks:
            frames.append(df.iloc[picks])
        global_offset += n

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def prepare_duckdb(filtered_part: pd.DataFrame, query_file: str):
    dest_part = "../data/tpch/parquet/filtered_part.parquet"
    filtered_part.to_parquet(dest_part, index=False, engine="pyarrow")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE part     AS SELECT * FROM read_parquet('{dest_part}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{FILE_LINEITEM}')")
    sql = open(query_file).read()
    return con, sql

def prepare_datafusion(filtered_part: pd.DataFrame, query_file: str):
    dest_part = "../data/tpch/parquet/filtered_part.parquet"
    filtered_part.to_parquet(dest_part, index=False, engine="pyarrow")

    ctx = SessionContext()
    ctx.register_parquet("part",     dest_part)
    ctx.register_parquet("lineitem", FILE_LINEITEM)
    sql = open(query_file).read()
    return ctx, sql

if __name__ == "__main__":
    os.makedirs(os.path.join(RESULT_DIR, "duckdb"),    exist_ok=True)
    os.makedirs(os.path.join(RESULT_DIR, "datafusion"), exist_ok=True)

    cppyy.add_include_path(".")
    cppyy.load_library("./fast/lib/libfast.so")
    cppyy.include("./fast/src/fast.hpp")

    fast_tree, cpp_entries, orig_bytes, build_secs = build_int_fast_tree(
        FILE_PART, BATCH, "p_size"
    )
    fast_tree_mb = fast_tree.getMemoryUsage() / (1024 * 1024)
    original_mb  = orig_bytes    / (1024 * 1024)

    lookup_metrics = measure_query_execution(
        lambda: fast_tree.rangeSearch(SIZE_MIN, SIZE_MAX, cpp_entries).entries
    )
    cpp_results = lookup_metrics["result"]
    filtered_indices = set(int(e.value) for e in cpp_results)

    filtered_part = materialize_filtered_indices(
        FILE_PART, filtered_indices, BATCH
    )

    con, sql = prepare_duckdb(filtered_part, QUERY_PATH)
    duck_metrics = measure_query_duckdb(19, con, sql)
    combined_duck = aggregate_metrics(lookup_metrics, duck_metrics)
    combined_duck.update({
        "Query": 19,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_duck = os.path.join(RESULT_DIR, "duckdb", "fast_tpch.csv")
    write_csv_results(out_duck, FIELDNAMES, [combined_duck])

    ctx, sql = prepare_datafusion(filtered_part, QUERY_PATH)
    df_metrics = measure_query_datafusion(19, ctx, sql)
    combined_df = aggregate_metrics(lookup_metrics, df_metrics)
    combined_df.update({
        "Query": 19,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_df = os.path.join(RESULT_DIR, "datafusion", "fast_tpch.csv")
    write_csv_results(out_df, FIELDNAMES, [combined_df])
