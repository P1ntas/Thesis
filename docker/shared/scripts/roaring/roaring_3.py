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
    write_csv_results
)
from common import measure_query_execution         

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.makedirs("../results", exist_ok=True)

NUM_RUNS = 3
BATCH    = 100_000

SEGMENT  = "BUILDING"
BEFORE   = pd.to_datetime("1995-03-15")
AFTER    = pd.to_datetime("1995-03-15")           


def combine(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_customer(path: str):
    idx_mkt = {}
    total_rows, orig_bytes = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH):
        df = batch.to_pandas()
        n = len(df)
        orig_bytes += df['c_mktsegment'].memory_usage(deep=True)

        for val in df['c_mktsegment'].unique():
            idx_mkt.setdefault(val, BitMap()).update(i + total_rows
                                                     for i in df.index[df['c_mktsegment'] == val])
        total_rows += n
        del df

    build_secs = time.perf_counter() - t0
    return idx_mkt, total_rows, orig_bytes, build_secs


def index_orders(path: str):
    idx_date = {}
    total_rows, orig_bytes = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH):
        df = batch.to_pandas()
        df['o_orderdate'] = pd.to_datetime(df['o_orderdate'])
        n = len(df)
        orig_bytes += df['o_orderdate'].memory_usage(deep=True)

        for val in df['o_orderdate'].unique():
            idx_date.setdefault(val, BitMap()).update(i + total_rows
                                                       for i in df.index[df['o_orderdate'] == val])
        total_rows += n
        del df

    build_secs = time.perf_counter() - t0
    return idx_date, total_rows, orig_bytes, build_secs


def index_line(path: str):
    idx_ship = {}
    total_rows, orig_bytes = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH):
        df = batch.to_pandas()
        df['l_shipdate'] = pd.to_datetime(df['l_shipdate'])
        n = len(df)
        orig_bytes += df['l_shipdate'].memory_usage(deep=True)

        for val in df['l_shipdate'].unique():
            idx_ship.setdefault(val, BitMap()).update(i + total_rows
                                                      for i in df.index[df['l_shipdate'] == val])
        total_rows += n
        del df

    build_secs = time.perf_counter() - t0
    return idx_ship, total_rows, orig_bytes, build_secs


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want   = iter(sorted(bitmap))
    cur    = next(want, None)
    out    = []
    offset = 0

    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH):
        n = len(batch)
        loc = []
        while cur is not None and cur < offset + n:
            loc.append(cur - offset)
            cur = next(want, None)
        if loc:
            out.append(batch.to_pandas()[cols].iloc[loc])
        offset += n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def date_range_bitmap(idx: dict, predicate):
    """Return OR of all bitmaps whose key satisfies predicate(date)."""
    return combine([bm for d, bm in idx.items() if predicate(d)])


def write_filtered_parquet(df, dest):
    df.to_parquet(dest, index=False, engine='pyarrow')
    return dest


def prepare_duckdb(c, o, l, sql_file):
    for col in ('l_extendedprice', 'l_quantity', 'l_discount', 'l_tax'):
        if col in l.columns:
            l[col] = l[col].astype('float64')

    dest_c = write_filtered_parquet(c, '../data/tpch/parquet/filtered_customer.parquet')
    dest_o = write_filtered_parquet(o, '../data/tpch/parquet/filtered_orders.parquet')
    dest_l = write_filtered_parquet(l, '../data/tpch/parquet/filtered_lineitem.parquet')

    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{dest_c}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{dest_o}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest_l}')")

    return con, open(sql_file).read()


def prepare_datafusion(c, o, l, sql_file):
    for col in ('l_extendedprice', 'l_quantity', 'l_discount', 'l_tax'):
        if col in l.columns:
            l[col] = l[col].astype('float64')

    dest_c = write_filtered_parquet(c, '../data/tpch/parquet/filtered_customer.parquet')
    dest_o = write_filtered_parquet(o, '../data/tpch/parquet/filtered_orders.parquet')
    dest_l = write_filtered_parquet(l, '../data/tpch/parquet/filtered_lineitem.parquet')

    ctx = SessionContext()
    ctx.register_parquet("customer", dest_c)
    ctx.register_parquet("orders",   dest_o)
    ctx.register_parquet("lineitem", dest_l)

    return ctx, open(sql_file).read()


if __name__ == "__main__":

    PATH_C = '../data/tpch/parquet/customer.parquet'
    PATH_O = '../data/tpch/parquet/orders.parquet'
    PATH_L = '../data/tpch/parquet/lineitem.parquet'

    mkt_idx, rows_c, bytes_c, sec_c = index_customer(PATH_C)
    date_idx, rows_o, bytes_o, sec_o = index_orders(PATH_O)
    ship_idx, rows_l, bytes_l, sec_l = index_line(PATH_L)

    all_cust = BitMap(range(rows_c))
    bm_cust  = measure_query_execution(
        lambda: all_cust & mkt_idx.get(SEGMENT, BitMap())
    )
    cust_bitmap = bm_cust["result"]

    all_orders = BitMap(range(rows_o))
    pred_dates = date_range_bitmap(date_idx, lambda d: d < BEFORE)
    bm_orders  = measure_query_execution(
        lambda: all_orders & pred_dates
    )
    orders_bitmap = bm_orders["result"]

    all_lines  = BitMap(range(rows_l))
    pred_ship  = date_range_bitmap(ship_idx, lambda d: d > AFTER)
    bm_lines   = measure_query_execution(
        lambda: all_lines & pred_ship
    )
    lines_bitmap = bm_lines["result"]

    df_c = materialise(PATH_C, cust_bitmap,
                       ['c_custkey', 'c_mktsegment'])
    df_o = materialise(PATH_O, orders_bitmap,
                       ['o_orderkey', 'o_custkey', 'o_orderdate', 'o_shippriority'])
    df_l = materialise(PATH_L, lines_bitmap,
                       ['l_orderkey', 'l_extendedprice', 'l_discount', 'l_shipdate'])

    bitmap_mb   = bitmap_memory_size(mkt_idx, date_idx, ship_idx)
    original_mb = (bytes_c + bytes_o + bytes_l) / (1024 * 1024)
    build_secs  = sec_c + sec_o + sec_l

    SQL_FILE = '../data/tpch/queries/3.sql'
    con, sql_duck = prepare_duckdb(df_c, df_o, df_l, SQL_FILE)
    res_duck = measure_query_duckdb(3, con, sql_duck)

    ctx, sql_df = prepare_datafusion(df_c, df_o, df_l, SQL_FILE)
    res_df   = measure_query_datafusion(3, ctx, sql_df)

    for tgt in (res_duck, res_df):
        for k in ("Latency (s)", "Peak Memory Usage (MB)",
                  "Average Memory Usage (MB)", "IOPS (ops/s)"):
            bm_total = ((bm_cust.get(k, 0) or 0) +
                        (bm_orders.get(k, 0) or 0) +
                        (bm_lines.get(k, 0) or 0))
            tgt[k] += bm_total / NUM_RUNS

        tgt.update({
            "Roaring Bitmap Size (MB)": bitmap_mb,
            "Original Columns Size (MB)": original_mb,
            "Bitmap Creation Time (s)": build_secs
        })
        for junk in ("result", "error"):
            tgt.pop(junk, None)

    HEADERS = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)"
    ]
    out_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    out_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(out_duck), exist_ok=True)
    os.makedirs(os.path.dirname(out_df),   exist_ok=True)

    write_csv_results(out_duck, HEADERS, [res_duck])
    write_csv_results(out_df,   HEADERS, [res_df])
