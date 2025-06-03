from __future__ import annotations

import argparse
import pathlib
import sys
from typing import List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

DEFAULT_PATHS = {
    "kdtree_datafusion": "../results/kdtree/datafusion/kdtree_tpch.csv",
    "kdtree_duckdb":     "../results/kdtree/duckdb/kdtree_tpch.csv",
    "tpch_duckdb":       "../results/tpch_duckdb.csv",
    "tpch_datafusion":   "../results/tpch_datafusion.csv",
    "out_dir":           "../results/kdtree/plots",
}

METRICS: List[Tuple[str, str, str]] = [
    ("Latency (s)",              "latency",    "Latency (s)"),
    ("CPU Usage (%)",            "cpu_usage",  "CPU Usage (%)"),
    ("Peak Memory Usage (MB)",   "peak_mem",   "Peak Memory Usage (MB)"),
    ("Average Memory Usage (MB)","avg_mem",    "Average Memory Usage (MB)"),
    ("IOPS (ops/s)",             "iops",       "IOPS (ops/s)"),
]

TREE_SIZE_METRICS = ("KD Tree Size (MB)", "Original Column Size (MB)")
KD_CREATION_METRIC = "KD Tree Creation Time (s)"



def _load_csv(path: str, needed_cols: List[str], label: str) -> pd.DataFrame:
    p = pathlib.Path(path)
    if not p.exists():
        sys.exit(f"[ERROR] {label} → file not found: {p.resolve()}")
    df = pd.read_csv(p)
    missing = set(needed_cols) - set(df.columns)
    if missing:
        sys.exit(
            f"[ERROR] {label} → missing columns {sorted(missing)} in {p.resolve()}"
        )
    return df


def _sort_queries(df: pd.DataFrame, query_col: str = "Query") -> pd.DataFrame:
    def key(val):
        num = "".join(ch for ch in str(val) if ch.isdigit())
        return int(num) if num else str(val)
    return df.assign(_k=df[query_col].map(key)).sort_values("_k").drop(columns="_k")


def _ensure_out_dir(path: pathlib.Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _plot_grouped_two_series(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    metric: str,
    series_left_label: str,
    series_right_label: str,
    outfile: pathlib.Path,
) -> None:
    merged = (
        df_left[["Query", metric]].rename(columns={metric: series_left_label})
        .merge(
            df_right[["Query", metric]].rename(columns={metric: series_right_label}),
            on="Query",
            how="inner",
        )
    )
    if merged.empty:
        return

    merged = _sort_queries(merged, "Query")
    queries = merged["Query"].astype(str).tolist()
    ind = np.arange(len(queries))
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(10, len(queries)), 6), dpi=100)
    ax.bar(ind, merged[series_left_label].values, width, label=series_left_label)
    ax.bar(ind + width, merged[series_right_label].values, width, label=series_right_label)

    ax.set_xlabel("Query")
    ax.set_ylabel(metric)
    ax.set_title(f"{metric} per Query — {series_left_label} vs {series_right_label}")
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(queries, rotation=45, ha="right")
    ax.legend()
    plt.savefig(outfile, bbox_inches="tight")
    plt.close(fig)


def _plot_tree_size(
    df_kd_db: pd.DataFrame,
    out_file: pathlib.Path,
) -> None:
    m1, m2 = TREE_SIZE_METRICS
    df = _sort_queries(df_kd_db[["Query", m1, m2]].copy(), "Query")
    queries = df["Query"].astype(str).tolist()
    ind = np.arange(len(queries))
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(10, len(queries)), 6), dpi=100)
    ax.bar(ind, df[m1].values, width, label=m1)
    ax.bar(ind + width, df[m2].values, width, label=m2)

    ax.set_xlabel("Query")
    ax.set_ylabel("Size (MB)")
    ax.set_title("KD-Tree vs Original Column Size per Query")
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(queries, rotation=45, ha="right")
    ax.legend()
    plt.savefig(out_file, bbox_inches="tight")
    plt.close(fig)


def _plot_kd_creation_time(
    df_kd: pd.DataFrame,
    engine_label: str,
    out_file: pathlib.Path,
) -> None:
    metric = KD_CREATION_METRIC
    df = _sort_queries(df_kd[["Query", metric]].copy(), "Query")
    queries = df["Query"].astype(str).tolist()
    ind = np.arange(len(queries))

    fig, ax = plt.subplots(figsize=(max(10, len(queries)), 6), dpi=100)
    ax.bar(ind, df[metric].values, width=0.6)
    ax.set_xlabel("Query")
    ax.set_ylabel(metric)
    ax.set_title(f"{metric} per Query ({engine_label})")
    ax.set_xticks(ind)
    ax.set_xticklabels(queries, rotation=45, ha="right")
    plt.savefig(out_file, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate TPCH KD-Tree comparison plots"
    )
    parser.add_argument("--kdtree-datafusion", default=DEFAULT_PATHS["kdtree_datafusion"])
    parser.add_argument("--kdtree-duckdb",     default=DEFAULT_PATHS["kdtree_duckdb"])
    parser.add_argument("--tpch-duckdb",       default=DEFAULT_PATHS["tpch_duckdb"])
    parser.add_argument("--tpch-datafusion",   default=DEFAULT_PATHS["tpch_datafusion"])
    parser.add_argument("--out-dir",           default=DEFAULT_PATHS["out_dir"])
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out_dir).resolve()
    _ensure_out_dir(out_dir)

    required_common = ["Query"] + [m[0] for m in METRICS]
    kd_extras = list(TREE_SIZE_METRICS) + [KD_CREATION_METRIC]

    df_kd_df = _load_csv(args.kdtree_datafusion,
                         required_common + kd_extras,
                         "KDTree DataFusion")
    df_kd_db = _load_csv(args.kdtree_duckdb,
                         required_common + kd_extras,
                         "KDTree DuckDB")
    df_base_db = _load_csv(args.tpch_duckdb,
                           required_common,
                           "Baseline DuckDB")
    df_base_df = _load_csv(args.tpch_datafusion,
                           required_common,
                           "Baseline DataFusion")

    for metric, stem, ylabel in METRICS:
        _plot_grouped_two_series(
            df_kd_db,
            df_base_db,
            metric,
            "KDTree DuckDB",
            "Baseline DuckDB",
            out_dir / f"{stem}_duckdb.png",
        )
        _plot_grouped_two_series(
            df_kd_df,
            df_base_df,
            metric,
            "KDTree DataFusion",
            "Baseline DataFusion",
            out_dir / f"{stem}_datafusion.png",
        )

    _plot_tree_size(df_kd_db, out_dir / "tree_size_duckdb.png")

    _plot_kd_creation_time(df_kd_db, "DuckDB",     out_dir / "kd_tree_creation_time.png")


if __name__ == "__main__":
    main()
