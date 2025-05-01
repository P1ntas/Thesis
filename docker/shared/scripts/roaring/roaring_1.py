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
from common import measure_query_execution   # <-- NEW

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.makedirs("../results", exist_ok=True)


def build_bitmap_indexes(file_path, batch_size):
    returnflag_idx, linestatus_idx = {}, {}
    global_offset, orig_bytes = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        df = batch.to_pandas()   
        n_rows = len(df)

        if 'l_returnflag' in df.columns:
            orig_bytes += df['l_returnflag'].memory_usage(deep=True)
        if 'l_linestatus' in df.columns:
            orig_bytes += df['l_linestatus'].memory_usage(deep=True)

        for col, idx in (('l_returnflag', returnflag_idx),
                         ('l_linestatus', linestatus_idx)):
            for v in df[col].unique():
                local = df.index[df[col] == v].tolist()
                idx.setdefault(v, BitMap()).update(i + global_offset for i in local)

        global_offset += n_rows
        del df         

    return (returnflag_idx,
            linestatus_idx,
            orig_bytes,
            time.perf_counter() - t0)


def materialise_filtered_df(file_path, bitmap, batch_size):
    if not bitmap:
        return pd.DataFrame()

    wanted = iter(sorted(bitmap))
    current = next(wanted, None)
    global_offset, out = 0, []

    for batch in pq.ParquetFile(file_path).iter_batches(batch_size=batch_size):
        n_rows = len(batch)
        local_idx = []
        while current is not None and current < global_offset + n_rows:
            local_idx.append(current - global_offset)
            current = next(wanted, None)

        if local_idx:
            out.append(batch.to_pandas().iloc[local_idx])

        global_offset += n_rows

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def prepare_duckdb(df, query_file):
    for col in ('l_extendedprice', 'l_quantity', 'l_discount', 'l_tax'):
        if col in df.columns:
            df[col] = df[col].astype('float64')

    dest = '../data/tpch/parquet/filtered_lineitem.parquet'
    df.to_parquet(dest, index=False, engine='pyarrow')

    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest}')")
    return con, open(query_file).read()


def prepare_datafusion(df, query_file):
    for col in ('l_extendedprice', 'l_quantity', 'l_discount', 'l_tax'):
        if col in df.columns:
            df[col] = df[col].astype('float64')

    dest = '../data/tpch/parquet/filtered_lineitem.parquet'
    df.to_parquet(dest, index=False, engine='pyarrow')
    ctx = SessionContext()
    ctx.register_parquet("lineitem", dest)
    return ctx, open(query_file).read()


if __name__ == "__main__":
    FILE = '../data/tpch/parquet/lineitem.parquet'
    BATCH = 100_000
    RET_FLAG, LINE_STAT = 'N', 'O'
    QUERY_PATH = '../data/tpch/queries/1.sql'

    (ret_idx, line_idx,
     orig_bytes, build_secs) = build_bitmap_indexes(FILE, BATCH)

    bitmap_metrics = measure_query_execution(
        lambda: ret_idx.get(RET_FLAG, BitMap()) & line_idx.get(LINE_STAT, BitMap())
    )
    filter_bitmap = bitmap_metrics["result"]

    filtered_df = materialise_filtered_df(FILE, filter_bitmap, BATCH)

    bitmap_mb = bitmap_memory_size(ret_idx, line_idx)
    original_mb = orig_bytes / (1024 * 1024)

    con, duck_query = prepare_duckdb(filtered_df, QUERY_PATH)
    r_duck = measure_query_duckdb(1, con, duck_query)

    for k in ("Latency (s)", "Peak Memory Usage (MB)",
              "Average Memory Usage (MB)", "IOPS (ops/s)"):
        if r_duck.get(k) is not None and bitmap_metrics.get(k) is not None:
            r_duck[k] += bitmap_metrics[k]

    r_duck.update({
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs
    })

    ctx, df_query = prepare_datafusion(filtered_df, QUERY_PATH)
    r_df = measure_query_datafusion(1, ctx, df_query)

    for k in ("Latency (s)", "Peak Memory Usage (MB)",
              "Average Memory Usage (MB)", "IOPS (ops/s)"):
        if r_df.get(k) is not None and bitmap_metrics.get(k) is not None:
            r_df[k] += bitmap_metrics[k]

    r_df.update({
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs
    })

    FIELDNAMES = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)"
    ]

    for junk in ("result", "error"):
        r_duck.pop(junk, None)

    duck_csv = "../results/roaring/duckdb/roaring_tpch.csv"
    os.makedirs(os.path.dirname(duck_csv), exist_ok=True)
    write_csv_results(duck_csv, FIELDNAMES, [r_duck])

    r_df.update({
    "Roaring Bitmap Size (MB)": bitmap_mb,
    "Original Columns Size (MB)": original_mb,
    "Bitmap Creation Time (s)": build_secs
    })

    for junk in ("result", "error"):
        r_df.pop(junk, None)

    df_csv = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(df_csv), exist_ok=True)
    write_csv_results(df_csv, FIELDNAMES, [r_df])
