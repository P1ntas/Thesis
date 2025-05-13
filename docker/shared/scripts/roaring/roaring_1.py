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
    write_csv_results,
    aggregate_metrics,         
)
from common import measure_query_execution


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.makedirs("../results", exist_ok=True)

FILE        = "../data/tpch/parquet/lineitem.parquet"
BATCH       = 100_000
RET_FLAG    = "N"
LINE_STAT   = "O"
QUERY_PATH  = "../data/tpch/queries/1.sql"


def build_bitmap_indexes(file_path: str, batch_size: int):
    returnflag_idx, linestatus_idx = {}, {}
    global_offset, orig_bytes      = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        df      = batch.to_pandas()
        n_rows  = len(df)

        if "l_returnflag" in df.columns:
            orig_bytes += df["l_returnflag"].memory_usage(deep=True)
        if "l_linestatus" in df.columns:
            orig_bytes += df["l_linestatus"].memory_usage(deep=True)

        for col, idx in (("l_returnflag", returnflag_idx),
                         ("l_linestatus", linestatus_idx)):
            for v in df[col].unique():
                local = df.index[df[col] == v].tolist()
                idx.setdefault(v, BitMap()).update(i + global_offset for i in local)

        global_offset += n_rows
        del df

    build_secs = time.perf_counter() - t0
    return returnflag_idx, linestatus_idx, orig_bytes, build_secs


def materialise_filtered_df(file_path: str, bitmap: BitMap, batch_size: int) -> pd.DataFrame:
    if not bitmap:
        return pd.DataFrame()

    wanted        = iter(sorted(bitmap))
    current       = next(wanted, None)
    global_offset = 0
    out           = []

    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        n_rows   = len(batch)
        local_ix = []
        while current is not None and current < global_offset + n_rows:
            local_ix.append(current - global_offset)
            current = next(wanted, None)

        if local_ix:
            out.append(batch.to_pandas().iloc[local_ix])

        global_offset += n_rows

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
    ret_idx, line_idx, orig_bytes, build_secs = build_bitmap_indexes(FILE, BATCH)

    bitmap_metrics = measure_query_execution(
        lambda: ret_idx.get(RET_FLAG, BitMap()) & line_idx.get(LINE_STAT, BitMap())
    )
    filtered_bitmap = bitmap_metrics["result"]
    filtered_df     = materialise_filtered_df(FILE, filtered_bitmap, BATCH)

    bitmap_mb   = bitmap_memory_size(ret_idx, line_idx)
    original_mb = orig_bytes / (1024 * 1024)


    con, sql_duck        = prepare_duckdb(filtered_df, QUERY_PATH)
    engine_metrics_duck  = measure_query_duckdb(1, con, sql_duck)

    combined_duck = aggregate_metrics(bitmap_metrics, engine_metrics_duck)

    for k, v in engine_metrics_duck.items():
        if k not in combined_duck:
            combined_duck[k] = v
    combined_duck.update(
        {
            "Query": 1,
            "Roaring Bitmap Size (MB)": bitmap_mb,
            "Original Columns Size (MB)": original_mb,
            "Bitmap Creation Time (s)": build_secs,
        }
    )
    combined_duck.pop("result", None)
    combined_duck.pop("error", None)


    ctx, sql_df          = prepare_datafusion(filtered_df, QUERY_PATH)
    engine_metrics_df    = measure_query_datafusion(1, ctx, sql_df)

    combined_df = aggregate_metrics(bitmap_metrics, engine_metrics_df)

    for k, v in engine_metrics_df.items():
        if k not in combined_df:
            combined_df[k] = v
    combined_df.update(
        {
            "Query": 1,
            "Roaring Bitmap Size (MB)": bitmap_mb,
            "Original Columns Size (MB)": original_mb,
            "Bitmap Creation Time (s)": build_secs,
        }
    )
    combined_df.pop("result", None)
    combined_df.pop("error", None)


    FIELDNAMES = [
        "Query",
        "Latency (s)",
        "CPU Usage (%)",
        "Peak Memory Usage (MB)",
        "Average Memory Usage (MB)",
        "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)",
        "Original Columns Size (MB)",
        "Bitmap Creation Time (s)",
    ]

    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, FIELDNAMES, [combined_duck])
    write_csv_results(out_df,   FIELDNAMES, [combined_df])
