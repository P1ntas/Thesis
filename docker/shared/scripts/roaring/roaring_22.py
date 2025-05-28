import os, sys, time
import duckdb, pyarrow.parquet as pq, pandas as pd
from functools import reduce
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

BATCH          = 6000000
NUM_RUNS_SQL   = 3
PREFIXES       = {"13", "31", "23", "29", "30", "18", "17"}


def or_reduce(bitmaps):
    return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_customer(path):
    idx_pref, idx_cust, idx_bal = {}, {}, {}
    rows, bytes_, pos_sum, pos_n = 0, 0, 0.0, 0
    t0 = time.perf_counter()

    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = batch.to_pandas(), len(batch)
        df["c_acctbal"] = df["c_acctbal"].astype(float)

        rows   += n
        bytes_ += (df["c_phone"].memory_usage(deep=True) +
                   df["c_custkey"].memory_usage(deep=True) +
                   df["c_acctbal"].memory_usage(deep=True))

        pref_series = df["c_phone"].str[:2]

        for pref in pref_series.unique():
            idx_pref.setdefault(pref, BitMap()).update(
                loc + rows - n for loc in pref_series.index[pref_series == pref]
            )

        for key, bal, loc in zip(df["c_custkey"], df["c_acctbal"], df.index):
            idx_cust.setdefault(key, BitMap()).add(loc + rows - n)
            idx_bal.setdefault(bal, BitMap()).add(loc + rows - n)

        good = (pref_series.isin(PREFIXES)) & (df["c_acctbal"] > 0.0)
        pos_sum += df.loc[good, "c_acctbal"].sum()
        pos_n   += int(good.sum())

    avg_bal   = pos_sum / pos_n if pos_n else 0.0
    build_sec = time.perf_counter() - t0
    return idx_pref, idx_cust, idx_bal, rows, bytes_, build_sec, avg_bal


def index_orders(path):
    idx_cust, rows, bytes_, t0 = {}, 0, 0, time.perf_counter()
    for batch in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = batch.to_pandas(), len(batch)
        rows  += n
        bytes_ += df["o_custkey"].memory_usage(deep=True)
        for key, loc in zip(df["o_custkey"], df.index):
            idx_cust.setdefault(key, BitMap()).add(loc + rows - n)
    return idx_cust, rows, bytes_, time.perf_counter() - t0


def materialise(path, bitmap, cols):
    if not bitmap:
        return pd.DataFrame(columns=cols)

    want, cur, offset, out = iter(sorted(bitmap)), None, 0, []
    cur = next(want, None)

    for b in pq.ParquetFile(path).iter_batches(BATCH):
        n, loc = len(b), []
        while cur is not None and cur < offset + n:
            loc.append(cur - offset)
            cur = next(want, None)
        if loc:
            out.append(b.to_pandas()[cols].iloc[loc])
        offset += n

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=cols)


def save_parquet(df, dest):
    df.to_parquet(dest, index=False, engine="pyarrow")
    return dest


def prep_duck(c_df, o_df, sql):
    c_df["c_acctbal"] = c_df["c_acctbal"].astype("float64")
    c_df["c_phone"]   = c_df["c_phone"].astype("string")

    pc = save_parquet(c_df, "../data/tpch/parquet/filtered_customer.parquet")
    po = save_parquet(o_df, "../data/tpch/parquet/filtered_orders.parquet")

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{pc}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{po}')")
    return con, open(sql).read()


def prep_df(c_df, o_df, sql):
    c_df["c_acctbal"] = c_df["c_acctbal"].astype("float64")
    c_df["c_phone"]   = c_df["c_phone"].astype("string")

    pc = save_parquet(c_df, "../data/tpch/parquet/filtered_customer.parquet")
    po = save_parquet(o_df, "../data/tpch/parquet/filtered_orders.parquet")

    ctx = SessionContext()
    ctx.register_parquet("customer", pc)
    ctx.register_parquet("orders",   po)
    return ctx, open(sql).read()


if __name__ == "__main__":
    PATH_C = "../data/tpch/parquet/customer.parquet"
    PATH_O = "../data/tpch/parquet/orders.parquet"

    (idx_pref, idx_cust, idx_bal,
     rows_c, bytes_c, sec_c, AVG_BAL) = index_customer(PATH_C)
    idx_ord, rows_o, bytes_o, sec_o   = index_orders(PATH_O)

    bm_prefix   = measure_query_execution(
        lambda: or_reduce([idx_pref[p] for p in PREFIXES if p in idx_pref])
    )
    bm_balance  = measure_query_execution(
        lambda: or_reduce([bm for bal, bm in idx_bal.items() if bal > AVG_BAL])
    )
    has_orders_bm = or_reduce(idx_ord.values())
    bm_no_orders = measure_query_execution(
        lambda: BitMap(range(rows_c)) - has_orders_bm
    )

    final_cust_bm = (bm_prefix["result"] &
                     bm_balance["result"] &
                     bm_no_orders["result"])

    df_customer = materialise(
        PATH_C, final_cust_bm,
        ["c_custkey", "c_acctbal", "c_phone"]
    )
    df_orders   = materialise(
        PATH_O, has_orders_bm,
        ["o_custkey"]
    )

    bitmap_mb   = bitmap_memory_size(idx_pref, idx_cust, idx_bal, idx_ord)
    original_mb = (bytes_c + bytes_o) / (1024 * 1024)
    build_secs  = sec_c + sec_o

    con, sql_duck  = prep_duck(df_customer, df_orders, "../data/tpch/queries/22.sql")
    eng_duck       = measure_query_duckdb(22, con, sql_duck, num_runs=NUM_RUNS_SQL)

    res_duck = aggregate_metrics(bm_prefix, bm_balance,
                                 bm_no_orders, eng_duck)     
    res_duck.update({
        "Query": 22,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_duck.pop("result", None); res_duck.pop("error", None)

    ctx, sql_df   = prep_df(df_customer, df_orders, "../data/tpch/queries/22.sql")
    eng_df        = measure_query_datafusion(22, ctx, sql_df, num_runs=NUM_RUNS_SQL)

    res_df = aggregate_metrics(bm_prefix, bm_balance,
                               bm_no_orders, eng_df)          
    res_df.update({
        "Query": 22,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_df.pop("result", None); res_df.pop("error", None)

    HEADERS = [
        "Query", "Latency (s)", "CPU Usage (%)",
        "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)",
        "Roaring Bitmap Size (MB)", "Original Columns Size (MB)",
        "Bitmap Creation Time (s)",
    ]
    dst_duck = "../results/roaring/duckdb/roaring_tpch.csv"
    dst_df   = "../results/roaring/datafusion/roaring_tpch.csv"
    os.makedirs(os.path.dirname(dst_duck), exist_ok=True)
    os.makedirs(os.path.dirname(dst_df),   exist_ok=True)

    write_csv_results(dst_duck, HEADERS, [res_duck])
    write_csv_results(dst_df,   HEADERS, [res_df])
