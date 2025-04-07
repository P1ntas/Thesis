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
    write_csv_results,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.makedirs("../results", exist_ok=True)

def process_lineitem_parquet(file_path, batch_size=100000):
    l_shipdate_index = {}
    l_discount_index = {}
    l_quantity_index = {}
    l_extendedprice_index = {}

    filtered_batches = []
    global_offset = 0

    start_date = pd.to_datetime("1994-01-01")
    end_date = pd.to_datetime("1995-01-01")
    discount_low = 0.05
    discount_high = 0.05
    quantity_threshold = 24

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        n_rows = len(df_batch)

        df_batch["l_shipdate"] = pd.to_datetime(df_batch["l_shipdate"])

        for col, index_dict in [
            ("l_shipdate", l_shipdate_index),
            ("l_discount", l_discount_index),
            ("l_quantity", l_quantity_index),
            ("l_extendedprice", l_extendedprice_index),
        ]:
            unique_vals = df_batch[col].unique()
            for val in unique_vals:
                local_indices = df_batch.index[df_batch[col] == val].tolist()
                global_indices = [i + global_offset for i in local_indices]
                if val not in index_dict:
                    index_dict[val] = BitMap(global_indices)
                else:
                    index_dict[val].update(global_indices)

        mask = (
            (df_batch["l_shipdate"] >= start_date)
            & (df_batch["l_shipdate"] < end_date)
            & (df_batch["l_discount"] >= discount_low)
            & (df_batch["l_discount"] <= discount_high)
            & (df_batch["l_quantity"] < quantity_threshold)
        )
        batch_filtered = df_batch.loc[mask, ["l_extendedprice", "l_discount", "l_quantity", "l_shipdate"]]
        filtered_batches.append(batch_filtered)

        global_offset += n_rows
        del df_batch  

    filtered_df = pd.concat(filtered_batches, ignore_index=True) if filtered_batches else pd.DataFrame()
    del filtered_batches 
    return (
        filtered_df,
        l_shipdate_index,
        l_discount_index,
        l_quantity_index,
        l_extendedprice_index,
    )


def prepare_duckdb(lineitem_df, query_file):
    if "l_extendedprice" in lineitem_df.columns:
        lineitem_df["l_extendedprice"] = lineitem_df["l_extendedprice"].astype("float64")
    if "l_discount" in lineitem_df.columns:
        lineitem_df["l_discount"] = lineitem_df["l_discount"].astype("float64")
    if "l_quantity" in lineitem_df.columns:
        lineitem_df["l_quantity"] = pd.to_numeric(lineitem_df["l_quantity"])

    filtered_parquet_path = "../data/tpch/parquet/filtered_lineitem.parquet"
    lineitem_df.to_parquet(filtered_parquet_path, index=False, engine="pyarrow")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{filtered_parquet_path}')")

    with open(query_file, "r") as f:
        query = f.read()

    return con, query


def prepare_datafusion(lineitem_df, query_file):
    if "l_extendedprice" in lineitem_df.columns:
        lineitem_df["l_extendedprice"] = lineitem_df["l_extendedprice"].astype("float64")
    if "l_discount" in lineitem_df.columns:
        lineitem_df["l_discount"] = lineitem_df["l_discount"].astype("float64")
    if "l_quantity" in lineitem_df.columns:
        lineitem_df["l_quantity"] = pd.to_numeric(lineitem_df["l_quantity"])

    filtered_parquet_path = "../data/tpch/parquet/filtered_lineitem.parquet"
    lineitem_df.to_parquet(filtered_parquet_path, index=False, engine="pyarrow")

    ctx = SessionContext()
    ctx.register_parquet("lineitem", filtered_parquet_path)

    with open(query_file, "r") as f:
        query = f.read()

    return ctx, query


if __name__ == "__main__":
    lineitem_parquet = "../data/tpch/parquet/lineitem.parquet"

    (
        lineitem_filtered_df,
        l_shipdate_index,
        l_discount_index,
        l_quantity_index,
        l_extendedprice_index,
    ) = process_lineitem_parquet(lineitem_parquet)

    bitmap_size_mb = bitmap_memory_size(
        l_shipdate_index, l_discount_index, l_quantity_index, l_extendedprice_index
    )

    sql_query_file = "../data/tpch/queries/6.sql"

    con_duckdb, duckdb_query = prepare_duckdb(lineitem_filtered_df, sql_query_file)
    result_duckdb = measure_query_duckdb(6, con_duckdb, duckdb_query)

    fieldnames = [
        "Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)", "IOPS (ops/s)", "Roaring Bitmap Size (MB)"
    ]
    duckdb_csv_result = {k: v for k, v in result_duckdb.items() if k in fieldnames}
    duckdb_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    duckdb_results_csv_path = "../results/roaring/duckdb/roaring_tpch.csv"
    os.makedirs(os.path.dirname(duckdb_results_csv_path), exist_ok=True)
    write_csv_results(duckdb_results_csv_path, fieldnames, [duckdb_csv_result])

    ctx_datafusion, datafusion_query = prepare_datafusion(lineitem_filtered_df, sql_query_file)
    result_datafusion = measure_query_datafusion(6, ctx_datafusion, datafusion_query)

    datafusion_csv_result = {k: v for k, v in result_datafusion.items() if k in fieldnames}
    datafusion_csv_result["Roaring Bitmap Size (MB)"] = bitmap_size_mb

    datafusion_results_csv_path = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(datafusion_results_csv_path), exist_ok=True)
    write_csv_results(datafusion_results_csv_path, fieldnames, [datafusion_csv_result])
