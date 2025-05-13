import os, sys, time
import duckdb, pyarrow.parquet as pq, pandas as pd
from pyroaring import BitMap
from datafusion import SessionContext
from functools import reduce

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

BATCH = 100_000
FROM  = pd.to_datetime("1994-01-01")
TO    = pd.to_datetime("1995-01-01")
DISC  = 0.05
QTY_LT = 24


def combine(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_line(path):
    idx_ship, idx_disc, idx_qty = {}, {}, {}
    rows, bytes_, t0 = 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b)
        df["l_shipdate"] = pd.to_datetime(df["l_shipdate"])
        bytes_ += (df["l_shipdate"].memory_usage(deep=True) +
                   df["l_discount"].memory_usage(deep=True) +
                   df["l_quantity"].memory_usage(deep=True))
        for col, idx in (("l_shipdate", idx_ship),
                         ("l_discount", idx_disc),
                         ("l_quantity", idx_qty)):
            for v in df[col].unique():
                idx.setdefault(v, BitMap()).update(
                    i + rows for i in df.index[df[col] == v]
                )
        rows += n
    return idx_ship, idx_disc, idx_qty, rows, bytes_, time.perf_counter() - t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)
    want, cur, off, out = iter(sorted(bitmap)), None, 0, []
    cur = next(want, None)
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n, loc = len(b), []
        while cur is not None and cur < off + n:
            loc.append(cur - off); cur = next(want, None)
        if loc: out.append(b.to_pandas()[cols].iloc[loc])
        off += n
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

    con, q_duck = prepare_duckdb(df_line, "../data/tpch/queries/6.sql")
    eng_duck    = measure_query_duckdb(6, con, q_duck)
    stage_build = {"Latency (s)": bm_build_time}            
    res_duck    = aggregate_metrics(stage_build, bm_filter, eng_duck)

    res_duck.update({
        "Query": 6,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_duck.pop("result", None); res_duck.pop("error", None)

    ctx, q_df = prepare_datafusion(df_line, "../data/tpch/queries/6.sql")
    eng_df    = measure_query_datafusion(6, ctx, q_df)
    res_df    = aggregate_metrics(stage_build, bm_filter, eng_df)

    res_df.update({
        "Query": 6,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_df.pop("result", None); res_df.pop("error", None)

    HEAD = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)",
    ]
    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, HEAD, [res_duck])
    write_csv_results(out_df,   HEAD, [res_df])
