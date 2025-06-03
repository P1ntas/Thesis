import os
import time
from typing import Dict, List, Set, Tuple

import duckdb
import cppyy
import pandas as pd
import pyarrow.parquet as pq

cppyy.add_include_path("./kdtree/src")
cppyy.include("vkdtree.hpp")
cppyy.load_library("./kdtree/lib/libfast.so")

from common_kdtree import (
    measure_query_duckdb,
    measure_query_datafusion,
    write_csv_results,
    aggregate_metrics,
)
from common import measure_query_execution
from datafusion import SessionContext

LINEITEM_FILE = "../data/tpch/parquet/lineitem.parquet"
PART_FILE = "../data/tpch/parquet/part.parquet"
BATCH = 6_000_000
QUERY_PATH = "../data/tpch/queries/19.sql"
RESULT_DIR = "../results/kdtree/"

FIELDNAMES = [
    "Query",
    "Latency (s)",
    "CPU Usage (%)",
    "Peak Memory Usage (MB)",
    "Average Memory Usage (MB)",
    "IOPS (ops/s)",
    "KD Tree Size (MB)",
    "Original Column Size (MB)",      
    "KD Tree Creation Time (s)",
]

SPECS: List[Tuple[str, List[str], int, int]] = [
    ("Brand#12", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1, 11),
    ("Brand#23", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10, 20),
    ("Brand#34", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20, 30),
]


def _encode(value: str, mapping: Dict[str, int]) -> int:
    if value not in mapping:
        mapping[value] = len(mapping)
    return mapping[value]


def _cast_numeric(df: pd.DataFrame):
    for col in ("l_extendedprice", "l_quantity", "l_discount", "l_tax"):
        if col in df.columns:
            df[col] = df[col].astype("float64")


def build_kd_tree(
    lineitem_path: str,
    part_path: str,
    batch_size: int,
):
    Entry3 = cppyy.gbl.vec.TripleEntry
    KD3 = cppyy.gbl.vec.TripleKdTree

    part_df = pq.read_table(
        part_path, columns=["p_partkey", "p_brand", "p_container"]
    ).to_pandas()
    part_lookup = part_df.set_index("p_partkey")[["p_brand", "p_container"]]

    brand_codes: Dict[str, int] = {}
    container_codes: Dict[str, int] = {}

    total_rows = pq.ParquetFile(lineitem_path).metadata.num_rows
    cpp_entries = cppyy.gbl.std.vector[Entry3]()
    cpp_entries.reserve(total_rows)

    enc_bytes = 0                  
    offset = 0
    t0 = time.perf_counter()

    pf = pq.ParquetFile(lineitem_path)
    for batch in pf.iter_batches(batch_size):
        df_li = batch.to_pandas()
        merged = df_li.join(part_lookup, on="l_partkey", how="inner")
        merged["l_quantity"] = merged["l_quantity"].astype("float32")

        enc_bytes += len(merged) * 3 * 4  

        for local_idx, row in merged.iterrows():
            brand_code = float(_encode(row["p_brand"], brand_codes))
            cont_code = float(_encode(row["p_container"], container_codes))
            qty = float(row["l_quantity"])

            e = Entry3()
            e.key = [brand_code, cont_code, qty]
            e.value = offset + local_idx
            cpp_entries.push_back(e)

        offset += len(merged)

    tree = KD3()
    tree.build(cpp_entries)
    build_secs = time.perf_counter() - t0

    return tree, brand_codes, container_codes, enc_bytes, build_secs


def materialize_filtered_indices(
    file_path: str, indices: Set[int], batch_size: int
) -> pd.DataFrame:
    if not indices:
        return pd.DataFrame()

    wanted = set(indices)
    out = []
    offset = 0
    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size):
        df = batch.to_pandas()
        n = len(df)
        local = [i for i in range(n) if (i + offset) in wanted]
        if local:
            out.append(df.iloc[local])
        offset += n

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def prepare_duckdb(df_lineitem: pd.DataFrame, part_file: str, query_file: str):
    _cast_numeric(df_lineitem)

    dest_li = "../data/tpch/parquet/filtered_lineitem.parquet"
    df_lineitem.to_parquet(dest_li, index=False, engine="pyarrow")

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE lineitem AS SELECT * FROM read_parquet(?)", [dest_li])
    con.execute(
        f"CREATE TABLE part AS SELECT * FROM read_parquet('{part_file}')"
    )
    return con, open(query_file).read()


def prepare_datafusion(df_lineitem: pd.DataFrame, part_file: str, query_file: str):
    _cast_numeric(df_lineitem)

    dest_li = "../data/tpch/parquet/filtered_lineitem.parquet"
    df_lineitem.to_parquet(dest_li, index=False, engine="pyarrow")

    ctx = SessionContext()
    ctx.register_parquet("lineitem", dest_li)
    ctx.register_parquet("part", part_file)
    return ctx, open(query_file).read()


def _collect_indices(kd_tree, brand_map, cont_map) -> Set[int]:
    indices: Set[int] = set()
    for brand, containers, qmin, qmax in SPECS:
        b_code = float(brand_map[brand])
        for cont in containers:
            c_code = float(cont_map[cont])
            res = kd_tree.rangeSearch(
                b_code, c_code, float(qmin),
                b_code, c_code, float(qmax),
            ).entries
            indices.update(int(e.value) for e in res)
    return indices


if __name__ == "__main__":
    os.makedirs(os.path.join(RESULT_DIR, "duckdb"), exist_ok=True)
    os.makedirs(os.path.join(RESULT_DIR, "datafusion"), exist_ok=True)

    kd_tree, brand_map, cont_map, enc_bytes, build_secs = build_kd_tree(
        LINEITEM_FILE, PART_FILE, BATCH
    )
    kd_tree_mb = kd_tree.getMemoryUsage() / (1024 * 1024)
    encoded_mb = enc_bytes / (1024 * 1024)     

    lookup_metrics = measure_query_execution(
        lambda: _collect_indices(kd_tree, brand_map, cont_map)
    )
    filtered_idx = set(lookup_metrics["result"])
    filtered_df = materialize_filtered_indices(LINEITEM_FILE, filtered_idx, BATCH)

    con, sql_duck = prepare_duckdb(filtered_df, PART_FILE, QUERY_PATH)
    eng_metrics_duck = measure_query_duckdb(19, con, sql_duck)
    combined_duck = aggregate_metrics(lookup_metrics, eng_metrics_duck)
    combined_duck.update(
        {
            "Query": 19,
            "KD Tree Size (MB)": kd_tree_mb,
            "Original Column Size (MB)": encoded_mb,  
            "KD Tree Creation Time (s)": build_secs,
        }
    )
    write_csv_results(
        os.path.join(RESULT_DIR, "duckdb", "kdtree_tpch.csv"),
        FIELDNAMES,
        [combined_duck],
    )

    ctx, sql_df = prepare_datafusion(filtered_df, PART_FILE, QUERY_PATH)
    eng_metrics_df = measure_query_datafusion(19, ctx, sql_df)
    combined_df = aggregate_metrics(lookup_metrics, eng_metrics_df)
    combined_df.update(
        {
            "Query": 19,
            "KD Tree Size (MB)": kd_tree_mb,
            "Original Column Size (MB)": encoded_mb,   
            "KD Tree Creation Time (s)": build_secs,
        }
    )
    write_csv_results(
        os.path.join(RESULT_DIR, "datafusion", "kdtree_tpch.csv"),
        FIELDNAMES,
        [combined_df],
    )
