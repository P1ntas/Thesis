import os
import sys
import time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
import cppyy
from datafusion import SessionContext
from datetime import datetime, date

from common_fast_tree import (
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
    aggregate_metrics,
)
from common import measure_query_execution

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.makedirs("../results", exist_ok=True)

cppyy.add_include_path(".")
cppyy.load_library("./fast/lib/libfast.so")
cppyy.include("./fast/src/fast.hpp")

FILE = "../data/tpch/parquet_bigger/limited_lineitem.parquet"
BATCH = 10_000
CUTOFF_DATE = "1998-12-01"  
QUERY_PATH = "../data/tpch/queries/1.sql"


def date_to_int32(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).days)


def build_date_fast_tree(file_path: str, batch_size: int, cutoff_date: str):
    FastTree = cppyy.gbl.fast.FastTree[3]
    Entry    = cppyy.gbl.fast.FastTree[3].Entry['int32_t', 'uint64_t']

    cpp_entries = cppyy.gbl.std.vector[Entry]()

    total = pq.ParquetFile(file_path).metadata.num_rows
    cpp_entries.reserve(total)

    cutoff_int = date_to_int32(cutoff_date)
    orig_bytes = 0
    global_off = 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        if "l_shipdate" in df.columns:
            col = df["l_shipdate"].dropna()
            orig_bytes += col.memory_usage(deep=True)
            for local_idx, ship_date in col.items():
                date_int = date_to_int32(ship_date.strftime("%Y-%m-%d"))
                row_idx  = global_off + local_idx
                cpp_entries.emplace_back(int(date_int), int(row_idx))
        global_off += len(df)

    fast_tree = FastTree()
    fast_tree.build(cpp_entries)
    build_secs = time.perf_counter() - t0
    return fast_tree, orig_bytes, build_secs, cutoff_int, cpp_entries


def filter_by_date_fast_tree(fast_tree, cutoff_int: int, original_entries):
    
    result = fast_tree.rangeLessThan(cutoff_int, original_entries)
    
    filtered_indices = set()
    for entry in result.entries:
        filtered_indices.add(int(entry.value))
    
    return filtered_indices


def materialize_filtered_indices(file_path: str, indices: set, batch_size: int) -> pd.DataFrame:
    if not indices:
        return pd.DataFrame()
    
    wanted = sorted(indices)
    wanted_set = set(wanted)
    global_offset = 0
    out = []
    
    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        n_rows = len(batch)
        local_ix = []
        
        for i in range(n_rows):
            if (i + global_offset) in wanted_set:
                local_ix.append(i)
        
        if local_ix:
            out.append(batch.to_pandas().iloc[local_ix])
        
        global_offset += n_rows
    
    result_df = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    return result_df


def _cast_numeric(df: pd.DataFrame):
    for col in ("l_extendedprice", "l_quantity", "l_discount", "l_tax"):
        if col in df.columns:
            df[col] = df[col].astype("float64")


def prepare_duckdb(df: pd.DataFrame, query_file: str):
    _cast_numeric(df)
    dest = "../data/tpch/parquet/filtered_lineitem_date.parquet"
    df.to_parquet(dest, index=False, engine="pyarrow")
    
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest}')")
    return con, open(query_file).read()


def prepare_datafusion(df: pd.DataFrame, query_file: str):
    _cast_numeric(df)
    dest = "../data/tpch/parquet/filtered_lineitem_date.parquet"
    df.to_parquet(dest, index=False, engine="pyarrow")
    
    ctx = SessionContext()
    ctx.register_parquet("lineitem", dest)
    return ctx, open(query_file).read()


if __name__ == "__main__":
    fast_tree, orig_bytes, build_secs, cutoff_int, cpp_entries = build_date_fast_tree(FILE, BATCH, CUTOFF_DATE)
    
    Entry = cppyy.gbl.fast.FastTree[3].Entry['int32_t', 'uint64_t']
    original_entries = cppyy.gbl.std.vector[Entry]()
    
    global_offset = 0
    for batch in pq.ParquetFile(FILE).iter_batches(batch_size=BATCH):
        df = batch.to_pandas()
        if "l_shipdate" in df.columns:
            for local_idx, ship_date in enumerate(df["l_shipdate"]):
                if pd.notna(ship_date):
                    date_str = ship_date.strftime("%Y-%m-%d")
                    date_int = date_to_int32(date_str)
                    global_idx = global_offset + local_idx
                    entry = Entry(int(date_int), int(global_idx))
                    original_entries.push_back(entry)
        global_offset += len(df)
        del df
    
    fast_tree_metrics = measure_query_execution(
        lambda: filter_by_date_fast_tree(fast_tree, cutoff_int, original_entries)
    )
    filtered_indices = fast_tree_metrics["result"]
    
    filtered_df = materialize_filtered_indices(FILE, filtered_indices, BATCH)
    
    fast_tree_mb = fast_tree.getMemoryUsage() / (1024 * 1024)
    original_mb = orig_bytes / (1024 * 1024)
    
    con, sql_duck = prepare_duckdb(filtered_df, QUERY_PATH)
    engine_metrics_duck = measure_query_duckdb(1, con, sql_duck)
    
    combined_duck = aggregate_metrics(fast_tree_metrics, engine_metrics_duck)
    
    for k, v in engine_metrics_duck.items():
        if k not in combined_duck:
            combined_duck[k] = v
    
    combined_duck.update({
        "Query": 1,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    
    combined_duck.pop("result", None)
    combined_duck.pop("error", None)
    
    ctx, sql_df = prepare_datafusion(filtered_df, QUERY_PATH)
    engine_metrics_df = measure_query_datafusion(1, ctx, sql_df)
    
    combined_df = aggregate_metrics(fast_tree_metrics, engine_metrics_df)
    
    for k, v in engine_metrics_df.items():
        if k not in combined_df:
            combined_df[k] = v
    
    combined_df.update({
        "Query": 1,
        "Fast Tree Size (MB)": fast_tree_mb,
        "Original Column Size (MB)": original_mb,
        "Fast Tree Creation Time (s)": build_secs,
    })
    
    combined_df.pop("result", None)
    combined_df.pop("error", None)
    
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
    
    out_duck = "../results/fast/duckdb/fast_tpch.csv"
    out_df = "../results/fast/datafusion/fast_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df), exist_ok=True)
    
    write_csv_results(out_duck, FIELDNAMES, [combined_duck])
    write_csv_results(out_df, FIELDNAMES, [combined_df])
