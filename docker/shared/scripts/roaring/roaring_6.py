import os, sys, time
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from pyroaring import BitMap
from datafusion import SessionContext
from functools import reduce

from common_roaring import (
    bitmap_memory_size,
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
)
from common import measure_query_execution          

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.makedirs("../results", exist_ok=True)

BATCH        = 100_000
NUM_RUNS_SQL = 3           

FROM = pd.to_datetime("1994-01-01")
TO   = pd.to_datetime("1995-01-01") 
DISC = 0.05
QTY_LT = 24


def combine(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_line(path: str):
    idx_ship, idx_disc, idx_qty = {}, {}, {}
    rows, bytes_ = 0, 0
    t0 = time.perf_counter()

    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df = b.to_pandas()
        df["l_shipdate"]  = pd.to_datetime(df["l_shipdate"])
        n = len(df)

        bytes_ += df["l_shipdate"].memory_usage(deep=True)
        bytes_ += df["l_discount"].memory_usage(deep=True)
        bytes_ += df["l_quantity"].memory_usage(deep=True)

        for col, idx in (("l_shipdate", idx_ship),
                         ("l_discount", idx_disc),
                         ("l_quantity",  idx_qty)):
            for v in df[col].unique():
                idx.setdefault(v, BitMap()).update(
                    i + rows for i in df.index[df[col] == v]
                )

        rows += n
        del df

    build_secs = time.perf_counter() - t0
    return idx_ship, idx_disc, idx_qty, rows, bytes_, build_secs


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want, out, offset = iter(sorted(bitmap)), [], 0
    cur = next(want, None)

    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n = len(b)
        loc = []
        while cur is not None and cur < offset + n:
            loc.append(cur - offset)
            cur = next(want, None)
        if loc:
            out.append(b.to_pandas()[cols].iloc[loc])
        offset += n

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def write_filtered(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prepare_duckdb(df_line, sql_file):
    for col in ("l_extendedprice", "l_discount", "l_quantity"):
        if col in df_line.columns:
            df_line[col] = df_line[col].astype("float64")

    dest = write_filtered(df_line, "../data/tpch/parquet/filtered_lineitem.parquet")
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest}')")
    return con, open(sql_file).read()


def prepare_datafusion(df_line, sql_file):
    for col in ("l_extendedprice", "l_discount", "l_quantity"):
        if col in df_line.columns:
            df_line[col] = df_line[col].astype("float64")

    dest = write_filtered(df_line, "../data/tpch/parquet/filtered_lineitem.parquet")
    ctx = SessionContext()
    ctx.register_parquet("lineitem", dest)
    return ctx, open(sql_file).read()


if __name__ == "__main__":

    PATH_L = "../data/tpch/parquet/lineitem.parquet"

    ship_idx, disc_idx, qty_idx, rows, bytes_, build_secs = index_line(PATH_L)

    t0 = time.perf_counter()
    ship_bm = combine([bm for d, bm in ship_idx.items() if FROM <= d < TO])
    disc_bm = disc_idx.get(DISC, BitMap())
    qty_bm  = combine([bm for q, bm in qty_idx.items() if q < QTY_LT])
    bm_build_time = time.perf_counter() - t0

    bm_filter = measure_query_execution(lambda: ship_bm & disc_bm & qty_bm)
    final_bitmap = bm_filter["result"]

    df_line = materialise(
        PATH_L, final_bitmap,
        ["l_extendedprice", "l_discount", "l_quantity", "l_shipdate"]
    )

    bitmap_mb   = bitmap_memory_size(ship_idx, disc_idx, qty_idx)
    original_mb = bytes_ / (1024 * 1024)

    SQL_FILE = "../data/tpch/queries/6.sql"
    con, sql_duck = prepare_duckdb(df_line, SQL_FILE)
    res_duck = measure_query_duckdb(6, con, sql_duck, num_runs=NUM_RUNS_SQL)

    ctx, sql_df = prepare_datafusion(df_line, SQL_FILE)
    res_df   = measure_query_datafusion(6, ctx, sql_df, num_runs=NUM_RUNS_SQL)

    for tgt in (res_duck, res_df):
        tgt["Latency (s)"] += (bm_build_time + bm_filter.get("Latency (s)", 0))

        tgt["IOPS (ops/s)"] += bm_filter.get("IOPS (ops/s)", 0)

        tgt["Peak Memory Usage (MB)"]    = max(
            tgt["Peak Memory Usage (MB)"],
            bm_filter.get("Peak Memory Usage (MB)", 0)
        )
        tgt["Average Memory Usage (MB)"] = max(
            tgt["Average Memory Usage (MB)"],
            bm_filter.get("Average Memory Usage (MB)", 0)
        )

        tgt.update({
            "Roaring Bitmap Size (MB)":      bitmap_mb,
            "Original Columns Size (MB)":    original_mb,
            "Bitmap Creation Time (s)":      build_secs,
        })

        for junk in ("result", "error"):
            tgt.pop(junk, None)

    HEADERS = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)",
    ]
    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, HEADERS, [res_duck])
    write_csv_results(out_df,   HEADERS, [res_df])
