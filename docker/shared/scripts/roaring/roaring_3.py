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

BATCH   = 6000000
SEGMENT = "BUILDING"
BEFORE  = pd.to_datetime("1995-03-15")
AFTER   = pd.to_datetime("1995-03-15")


def combine(bitmaps): return reduce(lambda a, b: a | b, bitmaps, BitMap())


def index_customer(path):
    idx, rows, bytes_, t0 = {}, 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b)
        bytes_ += df["c_mktsegment"].memory_usage(deep=True)
        for v in df["c_mktsegment"].unique():
            idx.setdefault(v, BitMap()).update(i + rows for i in df.index[df["c_mktsegment"] == v])
        rows += n
    return idx, rows, bytes_, time.perf_counter() - t0


def index_orders(path):
    idx, rows, bytes_, t0 = {}, 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b); df["o_orderdate"] = pd.to_datetime(df["o_orderdate"])
        bytes_ += df["o_orderdate"].memory_usage(deep=True)
        for d in df["o_orderdate"].unique():
            idx.setdefault(d, BitMap()).update(i + rows for i in df.index[df["o_orderdate"] == d])
        rows += n
    return idx, rows, bytes_, time.perf_counter() - t0


def index_line(path):
    idx, rows, bytes_, t0 = {}, 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b); df["l_shipdate"] = pd.to_datetime(df["l_shipdate"])
        bytes_ += df["l_shipdate"].memory_usage(deep=True)
        for d in df["l_shipdate"].unique():
            idx.setdefault(d, BitMap()).update(i + rows for i in df.index[df["l_shipdate"] == d])
        rows += n
    return idx, rows, bytes_, time.perf_counter() - t0


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


def prepare_duckdb(c, o, l, sql_file):
    for col in ("l_extendedprice", "l_quantity", "l_discount", "l_tax"):
        if col in l.columns: l[col] = l[col].astype("float64")
    dc = write_filtered(c, "../data/tpch/parquet/filtered_customer.parquet")
    do = write_filtered(o, "../data/tpch/parquet/filtered_orders.parquet")
    dl = write_filtered(l, "../data/tpch/parquet/filtered_lineitem.parquet")
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{dc}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{do}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dl}')")
    return con, open(sql_file).read()


def prepare_datafusion(c, o, l, sql_file):
    for col in ("l_extendedprice", "l_quantity", "l_discount", "l_tax"):
        if col in l.columns: l[col] = l[col].astype("float64")
    dc = write_filtered(c, "../data/tpch/parquet/filtered_customer.parquet")
    do = write_filtered(o, "../data/tpch/parquet/filtered_orders.parquet")
    dl = write_filtered(l, "../data/tpch/parquet/filtered_lineitem.parquet")
    ctx = SessionContext()
    ctx.register_parquet("customer", dc)
    ctx.register_parquet("orders",   do)
    ctx.register_parquet("lineitem", dl)
    return ctx, open(sql_file).read()


if __name__ == "__main__":
    PATH_C = "../data/tpch/parquet/customer.parquet"
    PATH_O = "../data/tpch/parquet/orders.parquet"
    PATH_L = "../data/tpch/parquet/lineitem.parquet"

    mkt_idx, rows_c, bytes_c, sec_c = index_customer(PATH_C)
    date_idx, rows_o, bytes_o, sec_o = index_orders(PATH_O)
    ship_idx, rows_l, bytes_l, sec_l = index_line(PATH_L)

    bm_cust = measure_query_execution(lambda:
        BitMap(range(rows_c)) & mkt_idx.get(SEGMENT, BitMap())
    )
    bm_orders = measure_query_execution(lambda:
        BitMap(range(rows_o)) & combine([bm for d, bm in date_idx.items() if d < BEFORE])
    )
    bm_lines = measure_query_execution(lambda:
        BitMap(range(rows_l)) & combine([bm for d, bm in ship_idx.items() if d > AFTER])
    )

    df_c = materialise(PATH_C, bm_cust["result"],
                       ["c_custkey", "c_mktsegment"])
    df_o = materialise(PATH_O, bm_orders["result"],
                       ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"])
    df_l = materialise(PATH_L, bm_lines["result"],
                       ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"])

    bitmap_mb   = bitmap_memory_size(mkt_idx, date_idx, ship_idx)
    original_mb = (bytes_c + bytes_o + bytes_l) / (1024 * 1024)
    build_secs  = sec_c + sec_o + sec_l

    con, sql_duck      = prepare_duckdb(df_c, df_o, df_l, "../data/tpch/queries/3.sql")
    eng_duck           = measure_query_duckdb(3, con, sql_duck)
    res_duck = aggregate_metrics(bm_cust, bm_orders, bm_lines, eng_duck)

    res_duck.update({
        "Query": 3,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_duck.pop("result", None); res_duck.pop("error", None)

    ctx, sql_df        = prepare_datafusion(df_c, df_o, df_l, "../data/tpch/queries/3.sql")
    eng_df             = measure_query_datafusion(3, ctx, sql_df)
    res_df  = aggregate_metrics(bm_cust, bm_orders, bm_lines, eng_df)
    res_df.update({
        "Query": 3,
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
