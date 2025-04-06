# roaring_3.py

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common_roaring import (
    process_parquet_with_roaring,
    bitmap_memory_size,
    prepare_duckdb,
    prepare_datafusion,
    measure_query_duckdb,
    measure_query_datafusion
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import write_csv_results

os.makedirs("../results", exist_ok=True)

if __name__ == "__main__":
    parquet_file_path = '../data/tpch/parquet/lineitem.parquet'
    sql_query_file = '../data/tpch/queries/3.sql'

    # Step 1: Process parquet file with roaring index
    filtered_df, bitmap_indices = process_parquet_with_roaring(parquet_file_path)
    bitmap_size_mb = bitmap_memory_size(bitmap_indices.values())

    # Step 2: DuckDB execution
    con_duckdb, duckdb_query = prepare_duckdb(filtered_df, sql_query_file)
    result_duckdb = measure_query_duckdb(3, con_duckdb, duckdb_query)

    duckdb_fieldnames = [
        "Query", "Latency (s)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)"
    ]
    duckdb_csv_result = {k: v for k, v in result_duckdb.items() if k in duckdb_fieldnames}
    duckdb_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    duckdb_results_csv_path = "../results/roaring/duckdb/roaring_3.csv"
    os.makedirs(os.path.dirname(duckdb_results_csv_path), exist_ok=True)
    write_csv_results(duckdb_results_csv_path, duckdb_fieldnames, [duckdb_csv_result])

    # Step 3: DataFusion execution
    ctx_datafusion, datafusion_query = prepare_datafusion(filtered_df, sql_query_file)
    result_datafusion = measure_query_datafusion(3, ctx_datafusion, datafusion_query)

    datafusion_fieldnames = [
        "Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)", "Roaring Bitmap Size (MB)"
    ]
    datafusion_csv_result = {k: v for k, v in result_datafusion.items() if k in datafusion_fieldnames}
    datafusion_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    datafusion_results_csv_path = "../results/roaring/datafusion/roaring_3.csv"
    os.makedirs(os.path.dirname(datafusion_results_csv_path), exist_ok=True)
    write_csv_results(datafusion_results_csv_path, datafusion_fieldnames, [datafusion_csv_result])
