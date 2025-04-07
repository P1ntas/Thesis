import os
import sys
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

def process_customer_parquet(file_path, batch_size=100000):
    c_custkey_index = {}
    c_phone_index = {}
    c_acctbal_index = {}

    filtered_batches = []
    global_offset = 0

    valid_prefixes = {"13", "31", "23", "29", "30", "18", "17"}

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        for col, index_dict in [
            ("c_custkey", c_custkey_index),
            ("c_phone", c_phone_index),
            ("c_acctbal", c_acctbal_index),
        ]:
            if col not in df_batch.columns:
                continue
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)

        if "c_phone" in df_batch.columns:
            mask = df_batch["c_phone"].str[:2].isin(valid_prefixes)
        else:
            mask = pd.Series([False]*n_rows)

        keep_cols = [
            "c_custkey", "c_name", "c_acctbal", 
            "c_phone", "c_address", "c_comment", "c_nationkey"
        ]
        existing_cols = [col for col in keep_cols if col in df_batch.columns]

        batch_filtered = df_batch.loc[mask, existing_cols]
        filtered_batches.append(batch_filtered)

        global_offset += n_rows
        del df_batch  
    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, c_custkey_index, c_phone_index, c_acctbal_index


def process_orders_parquet(file_path, batch_size=100000):
    o_custkey_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        if "o_custkey" in df_batch.columns:
            unique_vals = df_batch["o_custkey"].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch["o_custkey"] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in o_custkey_index:
                    o_custkey_index[val] = BitMap(global_indices)
                else:
                    o_custkey_index[val].update(global_indices)

        keep_cols = ["o_custkey"]
        existing_cols = [col for col in keep_cols if col in df_batch.columns]
        filtered_batches.append(df_batch[existing_cols])

        global_offset += n_rows
        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, o_custkey_index


def prepare_duckdb(customer_df, orders_df, query_file):
    if "c_acctbal" in customer_df.columns:
        customer_df["c_acctbal"] = customer_df["c_acctbal"].astype("float64")

    filtered_customer_parquet = "../data/tpch/parquet/filtered_customer.parquet"
    filtered_orders_parquet   = "../data/tpch/parquet/filtered_orders.parquet"

    customer_df.to_parquet(filtered_customer_parquet, index=False, engine="pyarrow")
    orders_df.to_parquet(filtered_orders_parquet, index=False, engine="pyarrow")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{filtered_customer_parquet}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{filtered_orders_parquet}')")

    with open(query_file, "r") as f:
        query = f.read()

    return con, query


def prepare_datafusion(customer_df, orders_df, query_file):
    if "c_acctbal" in customer_df.columns:
        customer_df["c_acctbal"] = customer_df["c_acctbal"].astype("float64")

    filtered_customer_parquet = "../data/tpch/parquet/filtered_customer.parquet"
    filtered_orders_parquet   = "../data/tpch/parquet/filtered_orders.parquet"

    customer_df.to_parquet(filtered_customer_parquet, index=False, engine="pyarrow")
    orders_df.to_parquet(filtered_orders_parquet, index=False, engine="pyarrow")

    ctx = SessionContext()
    ctx.register_parquet("customer", filtered_customer_parquet)
    ctx.register_parquet("orders", filtered_orders_parquet)

    with open(query_file, "r") as f:
        query = f.read()

    return ctx, query


if __name__ == "__main__":
    customer_parquet = "../data/tpch/parquet/customer.parquet"
    orders_parquet   = "../data/tpch/parquet/orders.parquet"

    customer_filtered_df, c_custkey_idx, c_phone_idx, c_acctbal_idx = process_customer_parquet(customer_parquet)

    orders_filtered_df, o_custkey_idx = process_orders_parquet(orders_parquet)

    bitmap_size_mb = bitmap_memory_size(
        c_custkey_idx, c_phone_idx, c_acctbal_idx, 
        o_custkey_idx
    )

    sql_query_file = "../data/tpch/queries/22.sql"

    con_duckdb, duckdb_query = prepare_duckdb(customer_filtered_df, orders_filtered_df, sql_query_file)
    result_duckdb = measure_query_duckdb(22, con_duckdb, duckdb_query)

    fieldnames = [
        "Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)", "Roaring Bitmap Size (MB)"
    ]
    duckdb_csv_result = {k: v for k, v in result_duckdb.items() if k in fieldnames}
    duckdb_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    duckdb_results_csv_path = "../results/roaring/duckdb/roaring_tpch.csv"
    os.makedirs(os.path.dirname(duckdb_results_csv_path), exist_ok=True)
    write_csv_results(duckdb_results_csv_path, fieldnames, [duckdb_csv_result])

    ctx_datafusion, datafusion_query = prepare_datafusion(customer_filtered_df, orders_filtered_df, sql_query_file)
    result_datafusion = measure_query_datafusion(22, ctx_datafusion, datafusion_query)

    datafusion_csv_result = {k: v for k, v in result_datafusion.items() if k in fieldnames}
    datafusion_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    datafusion_results_csv_path = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(datafusion_results_csv_path), exist_ok=True)
    write_csv_results(datafusion_results_csv_path, fieldnames, [datafusion_csv_result])
