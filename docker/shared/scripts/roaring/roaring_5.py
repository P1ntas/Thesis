import os, sys, time
import duckdb, pyarrow.parquet as pq, pandas as pd
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

BATCH  = 100_000
REGION = "ASIA"
FROM   = pd.to_datetime("1994-01-01")
TO     = pd.to_datetime("1995-01-01")


def index_region(path):
    idx, rows, bytes_, t0 = {}, 0, 0, time.perf_counter()
    for b in pq.ParquetFile(path).iter_batches(BATCH):
        df, n = b.to_pandas(), len(b)
        bytes_ += df["r_name"].memory_usage(deep=True)
        for v in df["r_name"].unique():
            idx.setdefault(v, BitMap()).update(i + rows for i in df.index[df["r_name"] == v])
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


def prepare_duckdb(r, n, c, o, s, l, sql):
    for col in ("l_extendedprice", "l_discount"):
        if col in l.columns:
            l[col] = l[col].astype("float64")
    dr = write_filtered(r, "../data/tpch/parquet/filtered_region.parquet")
    dn = write_filtered(n, "../data/tpch/parquet/filtered_nation.parquet")
    dc = write_filtered(c, "../data/tpch/parquet/filtered_customer.parquet")
    do = write_filtered(o, "../data/tpch/parquet/filtered_orders.parquet")
    ds = write_filtered(s, "../data/tpch/parquet/filtered_supplier.parquet")
    dl = write_filtered(l, "../data/tpch/parquet/filtered_lineitem.parquet")
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE region   AS SELECT * FROM read_parquet('{dr}')")
    con.execute(f"CREATE TABLE nation   AS SELECT * FROM read_parquet('{dn}')")
    con.execute(f"CREATE TABLE customer AS SELECT * FROM read_parquet('{dc}')")
    con.execute(f"CREATE TABLE orders   AS SELECT * FROM read_parquet('{do}')")
    con.execute(f"CREATE TABLE supplier AS SELECT * FROM read_parquet('{ds}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_parquet('{dl}')")
    return con, open(sql).read()


def prepare_datafusion(r, n, c, o, s, l, sql):
    for col in ("l_extendedprice", "l_discount"):
        if col in l.columns:
            l[col] = l[col].astype("float64")
    dr = write_filtered(r, "../data/tpch/parquet/filtered_region.parquet")
    dn = write_filtered(n, "../data/tpch/parquet/filtered_nation.parquet")
    dc = write_filtered(c, "../data/tpch/parquet/filtered_customer.parquet")
    do = write_filtered(o, "../data/tpch/parquet/filtered_orders.parquet")
    ds = write_filtered(s, "../data/tpch/parquet/filtered_supplier.parquet")
    dl = write_filtered(l, "../data/tpch/parquet/filtered_lineitem.parquet")
    ctx = SessionContext()
    ctx.register_parquet("region", dr)
    ctx.register_parquet("nation", dn)
    ctx.register_parquet("customer", dc)
    ctx.register_parquet("orders", do)
    ctx.register_parquet("supplier", ds)
    ctx.register_parquet("lineitem", dl)
    return ctx, open(sql).read()


if __name__ == "__main__":
    PATH_R = "../data/tpch/parquet/region.parquet"
    PATH_N = "../data/tpch/parquet/nation.parquet"
    PATH_C = "../data/tpch/parquet/customer.parquet"
    PATH_O = "../data/tpch/parquet/orders.parquet"
    PATH_S = "../data/tpch/parquet/supplier.parquet"
    PATH_L = "../data/tpch/parquet/lineitem.parquet"

    r_idx, rows_r, bytes_r, sec_r = index_region(PATH_R)
    o_idx, rows_o, bytes_o, sec_o = index_orders(PATH_O)

    bm_region = measure_query_execution(
        lambda: BitMap(range(rows_r)) & r_idx.get(REGION, BitMap())
    )
    bm_orders = measure_query_execution(
        lambda: BitMap(range(rows_o)) & BitMap().union(
            *[bm for d, bm in o_idx.items() if FROM <= d < TO]
        )
    )

    df_r = materialise(PATH_R, bm_region["result"], ["r_regionkey", "r_name"])
    df_n = pd.read_parquet(PATH_N)
    df_c = pd.read_parquet(PATH_C)
    df_o = materialise(PATH_O, bm_orders["result"],
                       ["o_orderkey", "o_custkey", "o_orderdate"])
    df_s = pd.read_parquet(PATH_S)
    df_l = pd.read_parquet(PATH_L)

    bitmap_mb   = bitmap_memory_size(r_idx, o_idx)
    original_mb = (bytes_r + bytes_o) / (1024 * 1024)
    build_secs  = sec_r + sec_o

    con, q_duck = prepare_duckdb(df_r, df_n, df_c, df_o, df_s, df_l,
                                 "../data/tpch/queries/5.sql")
    eng_duck    = measure_query_duckdb(5, con, q_duck)  
    res_duck    = aggregate_metrics(bm_region, bm_orders, eng_duck)      

    res_duck.update({
        "Query": 5,
        "Roaring Bitmap Size (MB)": bitmap_mb,
        "Original Columns Size (MB)": original_mb,
        "Bitmap Creation Time (s)": build_secs,
    })
    res_duck.pop("result", None); res_duck.pop("error", None)

    ctx, q_df  = prepare_datafusion(df_r, df_n, df_c, df_o, df_s, df_l,
                                    "../data/tpch/queries/5.sql")
    eng_df     = measure_query_datafusion(5, ctx, q_df)
    res_df     = aggregate_metrics(bm_region, bm_orders, eng_df)        
    res_df.update({
        "Query": 5,
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
