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


def process_region_parquet(file_path, batch_size=100000):
    r_name_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        unique_vals = df_batch["r_name"].unique()
        for val in unique_vals:
            local_indices = df_batch.index[df_batch["r_name"] == val].tolist()
            global_indices = [i + global_offset for i in local_indices]
            if val not in r_name_index:
                r_name_index[val] = BitMap(global_indices)
            else:
                r_name_index[val].update(global_indices)

        batch_filtered = df_batch[df_batch["r_name"] == "ASIA"]
        filtered_batches.append(batch_filtered[["r_name", "r_regionkey"]])

        global_offset += n_rows
        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, r_name_index


def process_nation_parquet(file_path, batch_size=100000):
    n_name_index = {}
    n_regionkey_index = {}
    n_nationkey_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        for col, index_dict in [
            ("n_name", n_name_index),
            ("n_regionkey", n_regionkey_index),
            ("n_nationkey", n_nationkey_index),
        ]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)

        filtered_batches.append(df_batch[["n_name", "n_nationkey", "n_regionkey"]])

        global_offset += n_rows
        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, n_name_index, n_nationkey_index, n_regionkey_index


def process_customer_parquet(file_path, batch_size=100000):
    c_custkey_index = {}
    c_nationkey_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        for col, index_dict in [
            ("c_custkey", c_custkey_index),
            ("c_nationkey", c_nationkey_index),
        ]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)

        filtered_batches.append(df_batch[["c_custkey", "c_nationkey"]])

        global_offset += n_rows
        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, c_custkey_index, c_nationkey_index


def process_orders_parquet(file_path, batch_size=100000):
    o_orderdate_index = {}
    o_orderkey_index = {}
    o_custkey_index = {}
    filtered_batches = []
    global_offset = 0

    start_date = pd.to_datetime("1994-01-01")
    end_date = pd.to_datetime("1995-01-01")

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        df_batch["o_orderdate"] = pd.to_datetime(df_batch["o_orderdate"])

        for col, index_dict in [
            ("o_orderdate", o_orderdate_index),
            ("o_orderkey", o_orderkey_index),
            ("o_custkey", o_custkey_index),
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
        batch_filtered = df_batch.loc[mask, ["o_orderkey", "o_custkey", "o_orderdate"]]
        filtered_batches.append(batch_filtered)

        global_offset += n_rows
        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, o_orderdate_index, o_orderkey_index, o_custkey_index


def process_supplier_parquet(file_path, batch_size=100000):
    s_suppkey_index = {}
    s_nationkey_index = {}
    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        for col, index_dict in [
            ("s_suppkey", s_suppkey_index),
            ("s_nationkey", s_nationkey_index),
        ]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)

        filtered_batches.append(df_batch[["s_suppkey", "s_nationkey"]])

        global_offset += n_rows
        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, s_suppkey_index, s_nationkey_index


def process_lineitem_parquet(file_path, batch_size=100000):
    l_orderkey_index = {}
    l_suppkey_index = {}
    l_extendedprice_index = {}
    l_discount_index = {}

    filtered_batches = []
    global_offset = 0

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        for col, index_dict in [
            ("l_orderkey", l_orderkey_index),
            ("l_suppkey", l_suppkey_index),
            ("l_extendedprice", l_extendedprice_index),
            ("l_discount", l_discount_index),
        ]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)

        filtered_batches.append(df_batch[["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"]])
        global_offset += n_rows
        del df_batch

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches
    return filtered_df, l_orderkey_index, l_suppkey_index, l_extendedprice_index, l_discount_index


def prepare_duckdb(
    region_df, nation_df, customer_df, orders_df, supplier_df, lineitem_df, query_file
):
    if "l_extendedprice" in lineitem_df.columns:
        lineitem_df["l_extendedprice"] = lineitem_df["l_extendedprice"].astype("float64")
    if "l_discount" in lineitem_df.columns:
        lineitem_df["l_discount"] = lineitem_df["l_discount"].astype("float64")

    filtered_region = "../data/tpch/parquet/filtered_region.parquet"
    filtered_nation = "../data/tpch/parquet/filtered_nation.parquet"
    filtered_customer = "../data/tpch/parquet/filtered_customer.parquet"
    filtered_orders = "../data/tpch/parquet/filtered_orders.parquet"
    filtered_supplier = "../data/tpch/parquet/filtered_supplier.parquet"
    filtered_lineitem = "../data/tpch/parquet/filtered_lineitem.parquet"

    region_df.to_parquet(filtered_region, index=False, engine="pyarrow")
    nation_df.to_parquet(filtered_nation, index=False, engine="pyarrow")
    customer_df.to_parquet(filtered_customer, index=False, engine="pyarrow")
    orders_df.to_parquet(filtered_orders, index=False, engine="pyarrow")
    supplier_df.to_parquet(filtered_supplier, index=False, engine="pyarrow")
    lineitem_df.to_parquet(filtered_lineitem, index=False, engine="pyarrow")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE region   AS SELECT * FROM read_parquet('{filtered_region}')")
    con.execute(f"CREATE TABLE nation   AS SELECT * FROM read_parquet('{filtered_nation}')")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{filtered_customer}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{filtered_orders}')")
    con.execute(f"CREATE TABLE supplier AS SELECT * FROM read_parquet('{filtered_supplier}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{filtered_lineitem}')")

    with open(query_file, "r") as f:
        query = f.read()

    return con, query


def prepare_datafusion(
    region_df, nation_df, customer_df, orders_df, supplier_df, lineitem_df, query_file
):
    if "l_extendedprice" in lineitem_df.columns:
        lineitem_df["l_extendedprice"] = lineitem_df["l_extendedprice"].astype("float64")
    if "l_discount" in lineitem_df.columns:
        lineitem_df["l_discount"] = lineitem_df["l_discount"].astype("float64")

    filtered_region = "../data/tpch/parquet/filtered_region.parquet"
    filtered_nation = "../data/tpch/parquet/filtered_nation.parquet"
    filtered_customer = "../data/tpch/parquet/filtered_customer.parquet"
    filtered_orders = "../data/tpch/parquet/filtered_orders.parquet"
    filtered_supplier = "../data/tpch/parquet/filtered_supplier.parquet"
    filtered_lineitem = "../data/tpch/parquet/filtered_lineitem.parquet"

    region_df.to_parquet(filtered_region, index=False, engine="pyarrow")
    nation_df.to_parquet(filtered_nation, index=False, engine="pyarrow")
    customer_df.to_parquet(filtered_customer, index=False, engine="pyarrow")
    orders_df.to_parquet(filtered_orders, index=False, engine="pyarrow")
    supplier_df.to_parquet(filtered_supplier, index=False, engine="pyarrow")
    lineitem_df.to_parquet(filtered_lineitem, index=False, engine="pyarrow")

    ctx = SessionContext()
    ctx.register_parquet("region", filtered_region)
    ctx.register_parquet("nation", filtered_nation)
    ctx.register_parquet("customer", filtered_customer)
    ctx.register_parquet("orders", filtered_orders)
    ctx.register_parquet("supplier", filtered_supplier)
    ctx.register_parquet("lineitem", filtered_lineitem)

    with open(query_file, "r") as f:
        query = f.read()

    return ctx, query


if __name__ == "__main__":
    region_parquet   = "../data/tpch/parquet/region.parquet"
    nation_parquet   = "../data/tpch/parquet/nation.parquet"
    customer_parquet = "../data/tpch/parquet/customer.parquet"
    orders_parquet   = "../data/tpch/parquet/orders.parquet"
    supplier_parquet = "../data/tpch/parquet/supplier.parquet"
    lineitem_parquet = "../data/tpch/parquet/lineitem.parquet"

    region_df, r_name_index = process_region_parquet(region_parquet)
    nation_df, n_name_index, n_nationkey_index, n_regionkey_index = process_nation_parquet(nation_parquet)
    customer_df, c_custkey_index, c_nationkey_index = process_customer_parquet(customer_parquet)
    orders_df, o_orderdate_index, o_orderkey_index, o_custkey_index = process_orders_parquet(orders_parquet)
    supplier_df, s_suppkey_index, s_nationkey_index = process_supplier_parquet(supplier_parquet)
    lineitem_df, l_orderkey_index, l_suppkey_index, l_extendedprice_index, l_discount_index = process_lineitem_parquet(lineitem_parquet)

    bitmap_size_mb = bitmap_memory_size(
        r_name_index,
        n_name_index, n_nationkey_index, n_regionkey_index,
        c_custkey_index, c_nationkey_index,
        o_orderdate_index, o_orderkey_index, o_custkey_index,
        s_suppkey_index, s_nationkey_index,
        l_orderkey_index, l_suppkey_index, l_extendedprice_index, l_discount_index
    )

    sql_query_file = "../data/tpch/queries/5.sql"

    con_duckdb, duckdb_query = prepare_duckdb(
        region_df, nation_df, customer_df,
        orders_df, supplier_df, lineitem_df,
        sql_query_file
    )
    result_duckdb = measure_query_duckdb(5, con_duckdb, duckdb_query)

    fieldnames = [
        "Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)", "Roaring Bitmap Size (MB)"
    ]
    duckdb_csv_result = {k: v for k, v in result_duckdb.items() if k in fieldnames}
    duckdb_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    duckdb_results_csv_path = "../results/roaring/duckdb/roaring_tpch.csv"
    os.makedirs(os.path.dirname(duckdb_results_csv_path), exist_ok=True)
    write_csv_results(duckdb_results_csv_path, fieldnames, [duckdb_csv_result])

    ctx_datafusion, datafusion_query = prepare_datafusion(
        region_df, nation_df, customer_df,
        orders_df, supplier_df, lineitem_df,
        sql_query_file
    )
    result_datafusion = measure_query_datafusion(5, ctx_datafusion, datafusion_query)

    datafusion_csv_result = {k: v for k, v in result_datafusion.items() if k in fieldnames}
    datafusion_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    datafusion_results_csv_path = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(datafusion_results_csv_path), exist_ok=True)
    write_csv_results(datafusion_results_csv_path, fieldnames, [datafusion_csv_result])
