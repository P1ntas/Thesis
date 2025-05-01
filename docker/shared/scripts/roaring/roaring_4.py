import os, sys, time
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
from common import measure_query_execution        

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.makedirs("../results", exist_ok=True)

NUM_RUNS = 3
BATCH    = 100_000

DATE_FROM = pd.to_datetime("1993-07-01")
DATE_TO   = pd.to_datetime("1993-10-01")         


def index_orders(path: str):
    idx_date = {}
    rows, bytes_ = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        df = batch.to_pandas()
        df['o_orderdate'] = pd.to_datetime(df['o_orderdate'])
        bytes_ += df['o_orderdate'].memory_usage(deep=True)

        for d in df['o_orderdate'].unique():
            idx_date.setdefault(d, BitMap()).update(i + rows
                                                    for i in df.index[df['o_orderdate'] == d])
        rows += len(df)
        del df
    return idx_date, rows, bytes_, time.perf_counter() - t0


def index_line(path: str):
    commit_lt_receipt = BitMap()
    rows, bytes_ = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        df = batch.to_pandas()
        df['l_commitdate']  = pd.to_datetime(df['l_commitdate'])
        df['l_receiptdate'] = pd.to_datetime(df['l_receiptdate'])
        bytes_ += df['l_commitdate'].memory_usage(deep=True)
        bytes_ += df['l_receiptdate'].memory_usage(deep=True)

        local = df.index[df['l_commitdate'] < df['l_receiptdate']].tolist()
        commit_lt_receipt.update(i + rows for i in local)

        rows += len(df)
        del df
    return commit_lt_receipt, rows, bytes_, time.perf_counter() - t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want   = iter(sorted(bitmap))
    cur    = next(want, None)
    out    = []
    offset = 0

    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        n = len(batch); loc = []
        while cur is not None and cur < offset + n:
            loc.append(cur - offset)
            cur = next(want, None)
        if loc:
            out.append(batch.to_pandas()[cols].iloc[loc])
        offset += n
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def write_filtered(df, dest):
    df.to_parquet(dest, index=False, engine='pyarrow')
    return dest


def prepare_duckdb(o_df, l_df, sql_file):
    dest_o = write_filtered(o_df, '../data/tpch/parquet/filtered_orders.parquet')
    dest_l = write_filtered(l_df, '../data/tpch/parquet/filtered_lineitem.parquet')

    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{dest_o}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dest_l}')")
    return con, open(sql_file).read()


def prepare_datafusion(o_df, l_df, sql_file):
    dest_o = write_filtered(o_df, '../data/tpch/parquet/filtered_orders.parquet')
    dest_l = write_filtered(l_df, '../data/tpch/parquet/filtered_lineitem.parquet')

    ctx = SessionContext()
    ctx.register_parquet("orders",   dest_o)
    ctx.register_parquet("lineitem", dest_l)
    return ctx, open(sql_file).read()


if __name__ == "__main__":

    PATH_O = '../data/tpch/parquet/orders.parquet'
    PATH_L = '../data/tpch/parquet/lineitem.parquet'

    date_idx, rows_o, bytes_o, sec_o = index_orders(PATH_O)
    commit_lt_bm, rows_l, bytes_l,  sec_l = index_line(PATH_L)

    all_orders = BitMap(range(rows_o))
    date_range_bm = BitMap()          
    for d, bm in date_idx.items():
        if DATE_FROM <= d < DATE_TO:
            date_range_bm |= bm

    bm_orders = measure_query_execution(lambda: all_orders & date_range_bm)
    orders_bitmap = bm_orders["result"]

    all_lines = BitMap(range(rows_l))
    bm_lines  = measure_query_execution(lambda: all_lines & commit_lt_bm)
    lines_bitmap = bm_lines["result"]

    df_o = materialise(PATH_O, orders_bitmap,
                       ['o_orderkey', 'o_orderpriority', 'o_orderdate'])
    df_l = materialise(PATH_L, lines_bitmap,
                       ['l_orderkey', 'l_commitdate', 'l_receiptdate'])

    
    bitmap_mb = bitmap_memory_size(
        date_idx,                 
        {"lt_commit": commit_lt_bm}  
    )
    original_mb = (bytes_o + bytes_l) / (1024 * 1024)
    build_secs  = sec_o + sec_l

    SQL_FILE = '../data/tpch/queries/4.sql'
    con, sql_duck = prepare_duckdb(df_o, df_l, SQL_FILE)
    res_duck = measure_query_duckdb(4, con, sql_duck)

    ctx, sql_df = prepare_datafusion(df_o, df_l, SQL_FILE)
    res_df   = measure_query_datafusion(4, ctx, sql_df)

    bm_total_latency = (
        (bm_orders['Latency (s)'] or 0)
      + (bm_lines ['Latency (s)'] or 0)
    ) / 2

    for tgt in (res_duck, res_df):
        tgt['Latency (s)'] += bm_total_latency

        tgt.update({
            "Roaring Bitmap Size (MB)": bitmap_mb,
            "Original Columns Size (MB)": original_mb,
            "Bitmap Creation Time (s)":   build_secs
        })

        tgt["Peak Memory Usage (MB)"]   = max(
            tgt["Peak Memory Usage (MB)"],
            bm_orders["Peak Memory Usage (MB)"],
            bm_lines ["Peak Memory Usage (MB)"]
        )
        tgt["Average Memory Usage (MB)"] = max(
            tgt["Average Memory Usage (MB)"],
            bm_orders["Average Memory Usage (MB)"],
            bm_lines ["Average Memory Usage (MB)"]
        )
        tgt["IOPS (ops/s)"]              = max(
            tgt["IOPS (ops/s)"],
            bm_orders["IOPS (ops/s)"],
            bm_lines ["IOPS (ops/s)"]
        )

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
