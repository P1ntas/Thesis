import os
import sys
import time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from pyroaring import BitMap
from datafusion import SessionContext

from common_roaring import (
    bitmap_memory_size,
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.makedirs("../results", exist_ok=True)

def process_orders_parquet(file_path, batch_size=100000):
    o_orderdate_index = {}
    o_orderpriority_index = {}
    filtered_batches = []
    global_offset = 0
    original_size_bytes = 0

    start_time = time.perf_counter()

    start_date = pd.to_datetime("1993-07-01")
    end_date   = pd.to_datetime("1993-10-01")

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)
        
        df_batch["o_orderdate"] = pd.to_datetime(df_batch["o_orderdate"])
        if "o_orderdate" in df_batch.columns:
            original_size_bytes += df_batch["o_orderdate"].memory_usage(deep=True)
        if "o_orderpriority" in df_batch.columns:
            original_size_bytes += df_batch["o_orderpriority"].memory_usage(deep=True)
        
        for col, index_dict in [
            ("o_orderdate", o_orderdate_index),
            ("o_orderpriority", o_orderpriority_index),
        ]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)
                    
        mask = (df_batch["o_orderdate"] >= start_date) & (df_batch["o_orderdate"] < end_date)
        batch_filtered = df_batch.loc[mask, ["o_orderkey", "o_orderpriority", "o_orderdate"]]
        filtered_batches.append(batch_filtered)
        
        global_offset += n_rows
        del df_batch

    end_time = time.perf_counter()
    orders_bitmap_time = end_time - start_time

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches

    return filtered_df, o_orderdate_index, o_orderpriority_index, original_size_bytes, orders_bitmap_time

def process_lineitem_parquet(file_path, batch_size=100000):
    l_commitdate_index = {}
    l_receiptdate_index = {}
    filtered_batches = []
    global_offset = 0
    original_size_bytes = 0

    start_time = time.perf_counter()

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)
        
        df_batch["l_commitdate"] = pd.to_datetime(df_batch["l_commitdate"])
        df_batch["l_receiptdate"] = pd.to_datetime(df_batch["l_receiptdate"])
        if "l_commitdate" in df_batch.columns:
            original_size_bytes += df_batch["l_commitdate"].memory_usage(deep=True)
        if "l_receiptdate" in df_batch.columns:
            original_size_bytes += df_batch["l_receiptdate"].memory_usage(deep=True)
        
        for col, index_dict in [
            ("l_commitdate", l_commitdate_index),
            ("l_receiptdate", l_receiptdate_index),
        ]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)
                    
        mask = df_batch["l_commitdate"] < df_batch["l_receiptdate"]
        batch_filtered = df_batch.loc[mask, ["l_orderkey", "l_commitdate", "l_receiptdate"]]
        filtered_batches.append(batch_filtered)
        
        global_offset += n_rows
        del df_batch

    end_time = time.perf_counter()
    lineitem_bitmap_time = end_time - start_time

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches

    return filtered_df, l_commitdate_index, l_receiptdate_index, original_size_bytes, lineitem_bitmap_time


def prepare_duckdb(orders_df, lineitem_df, query_file):
    for col in ['l_extendedprice', 'l_quantity', 'l_discount', 'l_tax']:
        if col in lineitem_df.columns:
            lineitem_df[col] = lineitem_df[col].astype('float64')
    
    filtered_orders_parquet = '../data/tpch/parquet/filtered_orders.parquet'
    filtered_lineitem_parquet = '../data/tpch/parquet/filtered_lineitem.parquet'
    
    orders_df.to_parquet(filtered_orders_parquet, index=False, engine='pyarrow')
    lineitem_df.to_parquet(filtered_lineitem_parquet, index=False, engine='pyarrow')
    
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE orders AS SELECT * FROM read_parquet('{filtered_orders_parquet}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{filtered_lineitem_parquet}')")
    
    with open(query_file, 'r') as f:
        query = f.read()
    
    return con, query

def prepare_datafusion(orders_df, lineitem_df, query_file):
    for col in ['l_extendedprice', 'l_quantity', 'l_discount', 'l_tax']:
        if col in lineitem_df.columns:
            lineitem_df[col] = lineitem_df[col].astype('float64')
    
    filtered_orders_parquet = '../data/tpch/parquet/filtered_orders.parquet'
    filtered_lineitem_parquet = '../data/tpch/parquet/filtered_lineitem.parquet'
    
    orders_df.to_parquet(filtered_orders_parquet, index=False, engine='pyarrow')
    lineitem_df.to_parquet(filtered_lineitem_parquet, index=False, engine='pyarrow')
    
    ctx = SessionContext()
    ctx.register_parquet("orders", filtered_orders_parquet)
    ctx.register_parquet("lineitem", filtered_lineitem_parquet)
    
    with open(query_file, 'r') as f:
        query = f.read()
    
    return ctx, query

if __name__ == "__main__":
    orders_parquet = '../data/tpch/parquet/orders.parquet'
    lineitem_parquet = '../data/tpch/parquet/lineitem.parquet'
    
    (orders_filtered_df,
     o_orderdate_idx,
     o_orderpriority_idx,
     orders_orig_size,
     orders_bitmap_time) = process_orders_parquet(orders_parquet)
    
    (lineitem_filtered_df,
     l_commitdate_idx,
     l_receiptdate_idx,
     lineitem_orig_size,
     lineitem_bitmap_time) = process_lineitem_parquet(lineitem_parquet)
    
    bitmap_size_mb = bitmap_memory_size(o_orderdate_idx, o_orderpriority_idx, l_commitdate_idx, l_receiptdate_idx)
    
    total_original_bytes = orders_orig_size + lineitem_orig_size
    original_size_mb = total_original_bytes / (1024.0 * 1024.0)
    
    total_bitmap_time = orders_bitmap_time + lineitem_bitmap_time
    
    sql_query_file = '../data/tpch/queries/4.sql'
    
    con_duckdb, duckdb_query = prepare_duckdb(orders_filtered_df, lineitem_filtered_df, sql_query_file)
    result_duckdb = measure_query_duckdb(4, con_duckdb, duckdb_query)
    
    fieldnames = [
        "Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)"
    ]
    
    duckdb_csv_result = {k: v for k, v in result_duckdb.items() if k in fieldnames}
    duckdb_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb
    duckdb_csv_result["Original Columns Size (MB)"] = original_size_mb
    duckdb_csv_result["Bitmap Creation Time (s)"] = total_bitmap_time
    
    duckdb_results_csv_path = "../results/roaring/duckdb/roaring_tpch.csv"
    os.makedirs(os.path.dirname(duckdb_results_csv_path), exist_ok=True)
    write_csv_results(duckdb_results_csv_path, fieldnames, [duckdb_csv_result])
    
    ctx_datafusion, datafusion_query = prepare_datafusion(orders_filtered_df, lineitem_filtered_df, sql_query_file)
    result_datafusion = measure_query_datafusion(4, ctx_datafusion, datafusion_query)
    
    datafusion_csv_result = {k: v for k, v in result_datafusion.items() if k in fieldnames}
    datafusion_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb
    datafusion_csv_result["Original Columns Size (MB)"] = original_size_mb
    datafusion_csv_result["Bitmap Creation Time (s)"] = total_bitmap_time
    
    datafusion_results_csv_path = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(datafusion_results_csv_path), exist_ok=True)
    write_csv_results(datafusion_results_csv_path, fieldnames, [datafusion_csv_result])
