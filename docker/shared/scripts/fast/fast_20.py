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

FILE_LINEITEM = "../data/tpch/parquet/lineitem.parquet"
FILE_PART     = "../data/tpch/parquet/part.parquet"
FILE_PARTSUPP = "../data/tpch/parquet/partsupp.parquet"
FILE_SUPPLIER = "../data/tpch/parquet/supplier.parquet"
FILE_NATION   = "../data/tpch/parquet/nation.parquet"

BATCH       = 100_000
START_DATE  = "1994-01-01"
END_DATE    = "1995-01-01"  
QUERY_PATH  = "../data/tpch/queries/20.sql"
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

def build_date_fast_tree(parquet_path: str, batch_size: int, date_col: str):
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

def _cast_numeric(df: pd.DataFrame):
    if "l_quantity" in df.columns:
        df["l_quantity"] = df["l_quantity"].astype("float64")

def prepare_duckdb(filtered_li: pd.DataFrame, query_file: str):
    _cast_numeric(filtered_li)
    dest_li = "../data/tpch/parquet/filtered_lineitem.parquet"
    filtered_li.to_parquet(dest_li, index=False, engine="pyarrow")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE lineitem  AS SELECT * FROM read_parquet('{dest_li}')")
    con.execute(f"CREATE TABLE part      AS SELECT * FROM read_parquet('{FILE_PART}')")
    con.execute(f"CREATE TABLE partsupp  AS SELECT * FROM read_parquet('{FILE_PARTSUPP}')")
    con.execute(f"CREATE TABLE supplier  AS SELECT * FROM read_parquet('{FILE_SUPPLIER}')")
    con.execute(f"CREATE TABLE nation    AS SELECT * FROM read_parquet('{FILE_NATION}')")
    sql = open(query_file).read()
    return con, sql

def prepare_datafusion(filtered_li: pd.DataFrame, query_file: str):
    _cast_numeric(filtered_li)
    dest_li = "../data/tpch/parquet/filtered_lineitem.parquet"
    filtered_li.to_parquet(dest_li, index=False, engine="pyarrow")

    ctx = SessionContext()
    ctx.register_parquet("lineitem",  dest_li)
    ctx.register_parquet("part",      FILE_PART)
    ctx.register_parquet("partsupp",  FILE_PARTSUPP)
    ctx.register_parquet("supplier",  FILE_SUPPLIER)
    ctx.register_parquet("nation",    FILE_NATION)
    sql = open(query_file).read()
    return ctx, sql

if __name__ == "__main__":
    os.makedirs(os.path.join(RESULT_DIR, "duckdb"),    exist_ok=True)
    os.makedirs(os.path.join(RESULT_DIR, "datafusion"), exist_ok=True)

    cppyy.add_include_path(".")
    cppyy.load_library("./fast/lib/libfast.so")
    cppyy.include("./fast/src/fast.hpp")

    fast_tree, cpp_entries, orig_bytes, build_secs = build_date_fast_tree(
        FILE_LINEITEM, BATCH, "l_shipdate"
    )
    fast_tree_mb = fast_tree.getMemoryUsage() / (1024 * 1024)
    original_mb  = orig_bytes    / (1024 * 1024)

    start_int = date_to_int32(START_DATE)
    end_int   = date_to_int32(END_DATE) - 1

    lookup_metrics = measure_query_execution(
        lambda: fast_tree.rangeSearch(start_int, end_int, cpp_entries).entries
    )
    cpp_results = lookup_metrics["result"]
    filtered_idx = set(int(e.value) for e in cpp_results)

    filtered_df = materialize_filtered_indices(
        FILE_LINEITEM, filtered_idx, BATCH
    )

    con, sql_q20 = prepare_duckdb(filtered_df, QUERY_PATH)
    duck_metrics = measure_query_duckdb(20, con, sql_q20)
    combined_duck = aggregate_metrics(lookup_metrics, duck_metrics)
    combined_duck.update({
        "Query": 20,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_duck = os.path.join(RESULT_DIR, "duckdb", "fast_tpch.csv")
    write_csv_results(out_duck, FIELDNAMES, [combined_duck])

    ctx, sql_q20_df = prepare_datafusion(filtered_df, QUERY_PATH)
    df_metrics = measure_query_datafusion(20, ctx, sql_q20_df)
    combined_df = aggregate_metrics(lookup_metrics, df_metrics)
    combined_df.update({
        "Query": 20,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    out_df = os.path.join(RESULT_DIR, "datafusion", "fast_tpch.csv")
    write_csv_results(out_df, FIELDNAMES, [combined_df])
