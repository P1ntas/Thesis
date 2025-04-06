# common_roaring.py

import os
import sys
import duckdb
import pandas as pd
from pyroaring import BitMap
from datafusion import SessionContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import measure_query_execution

def bitmap_memory_size(bitmap_dicts):
    return sum(len(b.serialize()) for bitmap_dict in bitmap_dicts for b in bitmap_dict.values()) / (1024 * 1024)

def process_parquet_with_roaring(file_path, batch_size=100_000, cardinality_threshold=100):
    """
    Build roaring bitmaps for low-cardinality columns from a Parquet file.
    Returns the full DataFrame and bitmap index dictionary.
    """
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(file_path)
    bitmap_indices = {}
    filtered_batches = []
    global_offset = 0

    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        # Find low-cardinality columns
        low_card_cols = [
            col for col in df_batch.columns
            if df_batch[col].nunique() <= cardinality_threshold
        ]

        # Build bitmap indices
        for col in low_card_cols:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if col not in bitmap_indices:
                    bitmap_indices[col] = {}
                if val not in bitmap_indices[col]:
                    bitmap_indices[col][val] = BitMap(global_indices)
                else:
                    bitmap_indices[col][val].update(global_indices)

        filtered_batches.append(df_batch)
        global_offset += n_rows

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    return filtered_df, bitmap_indices

def prepare_duckdb(filtered_df, query_file, table_name="lineitem"):
    filtered_parquet_path = f'../data/tpch/parquet/filtered_{table_name}.parquet'
    filtered_df.to_parquet(filtered_parquet_path, index=False, engine='pyarrow')

    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{filtered_parquet_path}')")

    with open(query_file, 'r') as f:
        query = f.read()

    return con, query

def prepare_datafusion(filtered_df, query_file, table_name="lineitem"):
    filtered_parquet_path = f'../data/tpch/parquet/filtered_{table_name}.parquet'
    filtered_df.to_parquet(filtered_parquet_path, index=False, engine='pyarrow')

    ctx = SessionContext()
    ctx.register_parquet(table_name, filtered_parquet_path)

    with open(query_file, 'r') as f:
        query = f.read()

    return ctx, query

def measure_query_duckdb(query_number: int, con, query):
    def exec_fn():
        return con.execute(query).fetchall()
    result = measure_query_execution(exec_fn)
    result["Query"] = query_number
    return result

def measure_query_datafusion(query_number: int, ctx, query_str):
    def exec_fn():
        df = ctx.sql(query_str)
        return df.collect()
    result = measure_query_execution(exec_fn)
    result["Query"] = query_number
    return result
