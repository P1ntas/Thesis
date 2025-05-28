import os
import sys
import time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
import cppyy
from datetime import datetime

cppyy.add_include_path("./kdtree/src")
cppyy.include("vkdtree.hpp")
cppyy.load_library("./kdtree/lib/libfast.so")

from common_kdtree import (
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
    aggregate_metrics,
)
from common import measure_query_execution
from datafusion import SessionContext

FILE        = "../data/tpch/parquet/lineitem.parquet"
BATCH       = 6000000
START_DATE  = "1994-01-01"
END_DATE    = "1995-01-01"
DISC        = 0.05
QTY_LT      = 24
QUERY_PATH  = "../data/tpch/queries/6.sql"
RESULT_DIR  = "../results/kdtree/"

FIELDNAMES = [
    "Query",
    "Latency (s)",
    "CPU Usage (%)",
    "Peak Memory Usage (MB)",
    "Average Memory Usage (MB)",
    "IOPS (ops/s)",
    "KD Tree Size (MB)",
    "Original Column Size (MB)",
    "KD Tree Creation Time (s)",
]

def date_to_int32(date_str: str) -> int:
    dt    = datetime.strptime(date_str, "%Y-%m-%d")
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).days)

def build_kd_tree(file_path: str,
                  batch_size: int,
                  date_col: str,
                  disc_col: str,
                  qty_col: str):

    Entry3 = cppyy.gbl.vec.TripleEntry
    KD3    = cppyy.gbl.vec.TripleKdTree

    total = pq.ParquetFile(file_path).metadata.num_rows
    cpp_entries = cppyy.gbl.std.vector[Entry3]()
    cpp_entries.reserve(total)

    orig_bytes = 0
    offset     = 0
    t0 = time.perf_counter()

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size):
        df = batch.to_pandas()
        df[date_col] = pd.to_datetime(df[date_col])
        df[disc_col] = df[disc_col].astype("float32")
        df[qty_col]  = df[qty_col].astype("float32")

        orig_bytes += (
            df[date_col].memory_usage(deep=True) +
            df[disc_col].memory_usage(deep=True) +
            df[qty_col].memory_usage(deep=True)
        )

        for local_idx, row in df.iterrows():
            e = Entry3()
            e.key = [
                float(date_to_int32(row[date_col].strftime("%Y-%m-%d"))),
                float(row[disc_col]),
                float(row[qty_col])
            ]
            e.value = offset + local_idx
            cpp_entries.push_back(e)

        offset += len(df)

    tree = KD3()
    tree.build(cpp_entries)
    build_secs = time.perf_counter() - t0

    return tree, cpp_entries, orig_bytes, build_secs

def materialize_filtered_indices(file_path: str,
                                 indices: set,
                                 batch_size: int) -> pd.DataFrame:

    if not indices:
        return pd.DataFrame()
    wanted = set(indices)
    out = []
    offset = 0
    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size):
        df = batch.to_pandas()
        n  = len(df)
        local = [i for i in range(n) if (i + offset) in wanted]
        if local:
            out.append(df.iloc[local])
        offset += n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def _cast_numeric(df: pd.DataFrame):
    for col in ("l_extendedprice", "l_quantity", "l_discount", "l_tax"):
        if col in df.columns:
            df[col] = df[col].astype("float64")

def prepare_duckdb(df: pd.DataFrame, query_file: str):
    _cast_numeric(df)
    dest = "../data/tpch/parquet/filtered_lineitem.parquet"
    df.to_parquet(dest, index=False, engine="pyarrow")
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest}')")
    return con, open(query_file).read()

def prepare_datafusion(df: pd.DataFrame, query_file: str):
    _cast_numeric(df)
    dest = "../data/tpch/parquet/filtered_lineitem.parquet"
    df.to_parquet(dest, index=False, engine="pyarrow")
    ctx = SessionContext()
    ctx.register_parquet("lineitem", dest)
    return ctx, open(query_file).read()

if __name__ == "__main__":
    os.makedirs(os.path.join(RESULT_DIR, "duckdb"),     exist_ok=True)
    os.makedirs(os.path.join(RESULT_DIR, "datafusion"), exist_ok=True)

    kd_tree, cpp_entries, orig_bytes, build_secs = build_kd_tree(
        FILE, BATCH, "l_shipdate", "l_discount", "l_quantity"
    )
    kd_tree_mb  = kd_tree.getMemoryUsage() / (1024 * 1024)
    original_mb = orig_bytes / (1024 * 1024)

    start_int    = date_to_int32(START_DATE)
    end_int_incl = date_to_int32(END_DATE) - 1

    lookup_metrics = measure_query_execution(
        lambda: kd_tree.rangeSearch(
            start_int, DISC,       0.0,
            end_int_incl, DISC,    float(QTY_LT)
        ).entries
    )
    cpp_result = lookup_metrics["result"]
    filtered_idx = set(int(e.value) for e in cpp_result)

    filtered_df = materialize_filtered_indices(FILE, filtered_idx, BATCH)

    con, sql_duck       = prepare_duckdb(filtered_df, QUERY_PATH)
    eng_metrics_duck    = measure_query_duckdb(6, con, sql_duck)
    combined_duck       = aggregate_metrics(lookup_metrics, eng_metrics_duck)
    combined_duck.update({
        "Query": 6,
        "KD Tree Size (MB)":             kd_tree_mb,
        "Original Column Size (MB)":     original_mb,
        "KD Tree Creation Time (s)":     build_secs,
    })
    write_csv_results(
        os.path.join(RESULT_DIR, "duckdb",  "kdtree_tpch.csv"),
        FIELDNAMES, [combined_duck]
    )

    ctx, sql_df          = prepare_datafusion(filtered_df, QUERY_PATH)
    eng_metrics_df       = measure_query_datafusion(6, ctx, sql_df)
    combined_df          = aggregate_metrics(lookup_metrics, eng_metrics_df)
    combined_df.update({
        "Query": 6,
        "KD Tree Size (MB)":             kd_tree_mb,
        "Original Column Size (MB)":     original_mb,
        "KD Tree Creation Time (s)":     build_secs,
    })
    write_csv_results(
        os.path.join(RESULT_DIR, "datafusion", "kdtree_tpch.csv"),
        FIELDNAMES, [combined_df]
    )
