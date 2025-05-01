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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.makedirs("../results", exist_ok=True)

NUM_RUNS = 3
BATCH    = 100_000
PREFIXES = {"13", "31", "23", "29", "30", "18", "17"}


def combine(bitmaps):
    """OR-reduce that also works on an empty list."""
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_customer(path: str):
    idx_pref, idx_cust, idx_bal = {}, {}, {}
    rows, bytes_   = 0, 0
    pos_sum, pos_n = 0.0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH):
        df   = batch.to_pandas()
        n    = len(df)
        rows += n

        df["c_acctbal"] = df["c_acctbal"].astype(float)

        bytes_ += (df["c_phone"].memory_usage(deep=True) +
                   df["c_custkey"].memory_usage(deep=True) +
                   df["c_acctbal"].memory_usage(deep=True))

        pref_series = df["c_phone"].str[:2]
        for pref in pref_series.unique():
            idx_pref.setdefault(pref, BitMap()).update(
                i + rows - n for i in pref_series.index[pref_series == pref]
            )

        for key, loc in zip(df["c_custkey"], df.index):
            idx_cust.setdefault(key, BitMap()).add(loc + rows - n)

        for bal, loc in zip(df["c_acctbal"], df.index):
            idx_bal.setdefault(bal, BitMap()).add(loc + rows - n)

        good_mask = (pref_series.isin(PREFIXES)) & (df["c_acctbal"] > 0.0)
        pos_sum  += df.loc[good_mask, "c_acctbal"].sum()
        pos_n    += int(good_mask.sum())

        del df

    build_secs = time.perf_counter() - t0
    avg_bal    = pos_sum / pos_n if pos_n else 0.0
    return (idx_pref, idx_cust, idx_bal,
            rows, bytes_, build_secs, avg_bal)


def index_orders(path: str):
    idx_cust = {}
    rows, bytes_ = 0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH):
        df   = batch.to_pandas(); n = len(df)
        rows += n
        bytes_ += df["o_custkey"].memory_usage(deep=True)

        for key, loc in zip(df["o_custkey"], df.index):
            idx_cust.setdefault(key, BitMap()).add(loc + rows - n)

        del df

    return idx_cust, rows, bytes_, time.perf_counter() - t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want = iter(sorted(bitmap))
    cur  = next(want, None)
    out, offset = [], 0

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


def write_filtered(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prepare_duckdb(cust_df, orders_df, sql_file):
    cust_df["c_acctbal"] = cust_df["c_acctbal"].astype("float64")
    cust_df["c_phone"]   = cust_df["c_phone"].astype("string")   # <--- FIX
    p_c = write_filtered(cust_df, "../data/tpch/parquet/filtered_customer.parquet")
    p_o = write_filtered(orders_df, "../data/tpch/parquet/filtered_orders.parquet")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{p_c}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{p_o}')")
    return con, open(sql_file).read()


def prepare_datafusion(cust_df, orders_df, sql_file):
    cust_df["c_acctbal"] = cust_df["c_acctbal"].astype("float64")
    cust_df["c_phone"]   = cust_df["c_phone"].astype("string")   # <--- FIX
    p_c = write_filtered(cust_df, "../data/tpch/parquet/filtered_customer.parquet")
    p_o = write_filtered(orders_df, "../data/tpch/parquet/filtered_orders.parquet")

    ctx = SessionContext()
    ctx.register_parquet("customer", p_c)
    ctx.register_parquet("orders",   p_o)
    return ctx, open(sql_file).read()


if __name__ == "__main__":

    PATH_C = "../data/tpch/parquet/customer.parquet"
    PATH_O = "../data/tpch/parquet/orders.parquet"

    (idx_pref, idx_cust, idx_bal,
     rows_c, bytes_c, sec_c, AVG_BAL) = index_customer(PATH_C)

    idx_ord, rows_o, bytes_o, sec_o  = index_orders(PATH_O)

    bm_prefix = measure_query_execution(
        lambda: combine([idx_pref[p] for p in PREFIXES if p in idx_pref])
    )

    bm_balance = measure_query_execution(
        lambda: combine([bm for bal, bm in idx_bal.items() if bal > AVG_BAL])
    )

    has_order_bm = combine(idx_ord.values())
    bm_no_orders = measure_query_execution(
        lambda: BitMap(range(rows_c)) - has_order_bm     
    )

    final_cust_bm = (bm_prefix["result"] &
                     bm_balance["result"] &
                     bm_no_orders["result"])

    df_customer = materialise(
        PATH_C, final_cust_bm,
        ["c_custkey", "c_acctbal", "c_phone"]
    )
    df_orders = materialise(PATH_O, has_order_bm, ["o_custkey"])

    bitmap_mb   = bitmap_memory_size(idx_pref, idx_cust, idx_bal, idx_ord)
    original_mb = (bytes_c + bytes_o) / (1024 * 1024)
    build_secs  = sec_c + sec_o

    SQL_FILE = "../data/tpch/queries/22.sql"
    con, q_duck = prepare_duckdb(df_customer, df_orders, SQL_FILE)
    res_duck    = measure_query_duckdb(22, con, q_duck)

    ctx, q_df   = prepare_datafusion(df_customer, df_orders, SQL_FILE)
    res_df      = measure_query_datafusion(22, ctx, q_df)

    for tgt in (res_duck, res_df):
        for k in ("Latency (s)", "Peak Memory Usage (MB)",
                  "Average Memory Usage (MB)", "IOPS (ops/s)"):
            base  = tgt.get(k, 0) or 0
            extra = ((bm_prefix.get(k, 0) or 0) +
                     (bm_balance.get(k, 0) or 0) +
                     (bm_no_orders.get(k, 0) or 0)) / NUM_RUNS
            tgt[k] = base + extra

        tgt.update({
            "Roaring Bitmap Size (MB)": bitmap_mb,
            "Original Columns Size (MB)": original_mb,
            "Bitmap Creation Time (s)":  build_secs
        })
        for junk in ("result", "error"):
            tgt.pop(junk, None)

    HEADERS = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)"
    ]
    dst_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    dst_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(dst_duck), exist_ok=True)
    os.makedirs(os.path.dirname(dst_df),   exist_ok=True)

    write_csv_results(dst_duck, HEADERS, [res_duck])
    write_csv_results(dst_df,   HEADERS, [res_df])
