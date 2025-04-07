import os
import sys
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from pyroaring import BitMap
from datafusion import SessionContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common import measure_query_execution, write_csv_results

os.makedirs("../results", exist_ok=True)

def process_customer_parquet(file_path, batch_size=100000):
    c_mktsegment_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)
        unique_vals = df_batch['c_mktsegment'].unique()
        for val in unique_vals:
            local_indices = df_batch.index[df_batch['c_mktsegment'] == val].tolist()
            global_indices = [i + global_offset for i in local_indices]
            if val not in c_mktsegment_index:
                c_mktsegment_index[val] = BitMap(global_indices)
            else:
                c_mktsegment_index[val].update(global_indices)

        batch_filtered = df_batch[df_batch['c_mktsegment'] == 'BUILDING']
        filtered_batches.append(batch_filtered[['c_custkey', 'c_mktsegment']])
        global_offset += n_rows

        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches 

    return filtered_df, c_mktsegment_index


def process_orders_parquet(file_path, batch_size=100000):

    target_date = pd.to_datetime("1995-03-15")
    o_orderdate_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()

        df_batch['o_orderdate'] = pd.to_datetime(df_batch['o_orderdate'])

        n_rows = len(df_batch)

        unique_vals = df_batch['o_orderdate'].unique()
        for val in unique_vals:
            local_indices = df_batch.index[df_batch['o_orderdate'] == val].tolist()
            global_indices = [i + global_offset for i in local_indices]
            if val not in o_orderdate_index:
                o_orderdate_index[val] = BitMap(global_indices)
            else:
                o_orderdate_index[val].update(global_indices)

        batch_filtered = df_batch[df_batch['o_orderdate'] < target_date]
        filtered_batches.append(
            batch_filtered[['o_orderkey', 'o_custkey', 'o_orderdate', 'o_shippriority']]
        )
        global_offset += n_rows

        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches

    return filtered_df, o_orderdate_index


def process_lineitem_parquet(file_path, batch_size=100000):
    l_shipdate_index = {}
    filtered_batches = []
    global_offset = 0

    cutoff_date = pd.to_datetime("1995-03-15")

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()

        df_batch['l_shipdate'] = pd.to_datetime(df_batch['l_shipdate'])

        n_rows = len(df_batch)
        
        unique_vals = df_batch['l_shipdate'].unique()
        for val in unique_vals:
            local_indices = df_batch.index[df_batch['l_shipdate'] == val].tolist()
            global_indices = [i + global_offset for i in local_indices]
            if val not in l_shipdate_index:
                l_shipdate_index[val] = BitMap(global_indices)
            else:
                l_shipdate_index[val].update(global_indices)

        batch_filtered = df_batch[df_batch['l_shipdate'] > cutoff_date]
        filtered_batches.append(
            batch_filtered[['l_orderkey', 'l_extendedprice', 'l_discount', 'l_shipdate']]
        )

        global_offset += n_rows
        del df_batch 

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches  

    return filtered_df, l_shipdate_index


def bitmap_memory_size(*bitmap_dicts):
    total_bytes = 0
    for bitmap_dict in bitmap_dicts:
        for bitmap in bitmap_dict.values():
            total_bytes += len(bitmap.serialize())
    return total_bytes / (1024 * 1024)


def prepare_duckdb(cust_df, orders_df, lineitem_df, query_file):
    if 'l_extendedprice' in lineitem_df.columns:
        lineitem_df['l_extendedprice'] = lineitem_df['l_extendedprice'].astype('float64')
    if 'l_discount' in lineitem_df.columns:
        lineitem_df['l_discount'] = lineitem_df['l_discount'].astype('float64')

    filtered_customer_parquet = '../data/tpch/parquet/filtered_customer.parquet'
    filtered_orders_parquet = '../data/tpch/parquet/filtered_orders.parquet'
    filtered_lineitem_parquet = '../data/tpch/parquet/filtered_lineitem.parquet'

    cust_df.to_parquet(filtered_customer_parquet, index=False, engine='pyarrow')
    orders_df.to_parquet(filtered_orders_parquet, index=False, engine='pyarrow')
    lineitem_df.to_parquet(filtered_lineitem_parquet, index=False, engine='pyarrow')

    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{filtered_customer_parquet}')")
    con.execute(f"CREATE TABLE orders AS SELECT * FROM read_parquet('{filtered_orders_parquet}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{filtered_lineitem_parquet}')")

    with open(query_file, 'r') as f:
        query = f.read()

    return con, query


def measure_query_duckdb(query_number: int, con, query):
    def exec_fn():
        return con.execute(query).fetchall()

    result = measure_query_execution(exec_fn)
    result["Query"] = query_number
    return result


def prepare_datafusion(cust_df, orders_df, lineitem_df, query_file):
    if 'l_extendedprice' in lineitem_df.columns:
        lineitem_df['l_extendedprice'] = lineitem_df['l_extendedprice'].astype('float64')
    if 'l_discount' in lineitem_df.columns:
        lineitem_df['l_discount'] = lineitem_df['l_discount'].astype('float64')

    filtered_customer_parquet = '../data/tpch/parquet/filtered_customer.parquet'
    filtered_orders_parquet = '../data/tpch/parquet/filtered_orders.parquet'
    filtered_lineitem_parquet = '../data/tpch/parquet/filtered_lineitem.parquet'

    cust_df.to_parquet(filtered_customer_parquet, index=False, engine='pyarrow')
    orders_df.to_parquet(filtered_orders_parquet, index=False, engine='pyarrow')
    lineitem_df.to_parquet(filtered_lineitem_parquet, index=False, engine='pyarrow')

    ctx = SessionContext()
    ctx.register_parquet("customer", filtered_customer_parquet)
    ctx.register_parquet("orders", filtered_orders_parquet)
    ctx.register_parquet("lineitem", filtered_lineitem_parquet)

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
    customer_parquet = '../data/tpch/parquet/customer.parquet'
    orders_parquet = '../data/tpch/parquet/orders.parquet'
    lineitem_parquet = '../data/tpch/parquet/lineitem.parquet'

    cust_filtered_df, c_mktsegment_index = process_customer_parquet(customer_parquet)
    orders_filtered_df, o_orderdate_index = process_orders_parquet(orders_parquet)
    lineitem_filtered_df, l_shipdate_index = process_lineitem_parquet(lineitem_parquet)

    bitmap_size_mb = bitmap_memory_size(c_mktsegment_index, o_orderdate_index, l_shipdate_index)

    sql_query_file = '../data/tpch/queries/3.sql'

    con_duckdb, duckdb_query = prepare_duckdb(
        cust_filtered_df, orders_filtered_df, lineitem_filtered_df, sql_query_file
    )
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

    ctx_datafusion, datafusion_query = prepare_datafusion(
        cust_filtered_df, orders_filtered_df, lineitem_filtered_df, sql_query_file
    )
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
