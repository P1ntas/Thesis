import os
import sys
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from pyroaring import BitMap
from datafusion import SessionContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import measure_query_execution, read_tpch_queries, write_csv_results

os.makedirs("../results", exist_ok=True)

def process_lineitem_parquet(file_path, batch_size=100000,
                             filter_returnflag='N', filter_linestatus='O'):
    returnflag_index = {}
    linestatus_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        for col, index_dict in [('l_returnflag', returnflag_index),
                                ('l_linestatus', linestatus_index)]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)

        batch_filtered = df_batch[
            (df_batch['l_returnflag'] == filter_returnflag) &
            (df_batch['l_linestatus'] == filter_linestatus)
        ]
        filtered_batches.append(batch_filtered)

        global_offset += n_rows

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    return filtered_df, returnflag_index, linestatus_index

def bitmap_memory_size(*bitmap_dicts):
    return sum(len(bitmap.serialize()) for bitmap_dict in bitmap_dicts for bitmap in bitmap_dict.values())

def prepare_duckdb(filtered_df, query_file):
    for col in ['l_extendedprice', 'l_quantity', 'l_discount', 'l_tax']:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].astype('float64')

    filtered_parquet_path = '../data/tpch/parquet/filtered_lineitem.parquet'
    filtered_df.to_parquet(filtered_parquet_path, index=False, engine='pyarrow')

    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{filtered_parquet_path}')")

    with open(query_file, 'r') as f:
        query = f.read()

    return con, query

def measure_query_duckdb(query_number: int, con, query):
    def exec_fn():
        return con.execute(query).fetchall()

    result = measure_query_execution(exec_fn)
    result["Query"] = query_number
    return result

def prepare_datafusion(filtered_df, query_file):
    for col in ['l_extendedprice', 'l_quantity', 'l_discount', 'l_tax']:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].astype('float64')

    filtered_parquet_path = '../data/tpch/parquet/filtered_lineitem.parquet'
    filtered_df.to_parquet(filtered_parquet_path, index=False, engine='pyarrow')

    ctx = SessionContext()

    ctx.register_parquet("lineitem", filtered_parquet_path)

    with open(query_file, 'r') as f:
        query = f.read()

    return ctx, query

def measure_query_datafusion(query_number: int, ctx, query_str):
    def exec_fn():
        df = ctx.sql(query_str)
        return df.collect()  

    result = measure_query_execution(exec_fn)
    result["Query"] = query_number
    return result

if __name__ == "__main__":
    file_path = '../data/tpch/parquet/lineitem.parquet'
    filtered_df, returnflag_idx, linestatus_idx = process_lineitem_parquet(file_path)

    bitmap_size_bytes = bitmap_memory_size(returnflag_idx, linestatus_idx)

    sql_query_file = '../data/tpch/queries/1.sql'

    con_duckdb, duckdb_query = prepare_duckdb(filtered_df, sql_query_file)
    result_duckdb = measure_query_duckdb(1, con_duckdb, duckdb_query)

    duckdb_fieldnames = [
        "Query", "Latency (s)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (bytes)"
    ]
    duckdb_csv_result = {k: v for k, v in result_duckdb.items() if k in duckdb_fieldnames}
    duckdb_csv_result["Roaring Bitmap Size (bytes)"] = bitmap_size_bytes

    duckdb_results_csv_path = "../results/roaring/duckdb/roaring_1.csv"
    os.makedirs(os.path.dirname(duckdb_results_csv_path), exist_ok=True)
    write_csv_results(duckdb_results_csv_path, duckdb_fieldnames, [duckdb_csv_result])

    ctx_datafusion, datafusion_query = prepare_datafusion(filtered_df, sql_query_file)
    result_datafusion = measure_query_datafusion(1, ctx_datafusion, datafusion_query)

    datafusion_fieldnames = [
        "Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (bytes)"
    ]
    datafusion_csv_result = {k: v for k, v in result_datafusion.items() if k in datafusion_fieldnames}
    datafusion_csv_result["Roaring Bitmap Size (bytes)"] = bitmap_size_bytes

    datafusion_results_csv_path = "../results/roaring/datafusion/roaring_1.csv"
    os.makedirs(os.path.dirname(datafusion_results_csv_path), exist_ok=True)
    write_csv_results(datafusion_results_csv_path, datafusion_fieldnames, [datafusion_csv_result])
