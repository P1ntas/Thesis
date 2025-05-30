import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

if len(sys.argv) < 2:
    print("Usage: python3 plots.py [tpch|tpcds]")
    sys.exit(1)

prefix = sys.argv[1].lower()
if prefix not in ['tpch', 'tpcds']:
    print("Error: Prefix must be either 'tpch' or 'tpcds'.")
    sys.exit(1)

os.makedirs("../results/plots", exist_ok=True)

df_datafusion = pd.read_csv(f"../results/{prefix}_datafusion.csv")
df_duckdb     = pd.read_csv(f"../results/{prefix}_duckdb.csv")

merged_df = pd.merge(
    df_datafusion,
    df_duckdb,
    on="Query",
    suffixes=("_DF", "_DuckDB")
)

metrics = [
    "Latency (s)",
    "Peak Memory Usage (MB)",
    "Average Memory Usage (MB)",
    "IOPS (ops/s)",
    "CPU Usage (%)"
]
width = 0.35

if prefix == "tpch":
    y = np.arange(len(merged_df["Query"]))
    for metric in metrics:
        fig, ax = plt.subplots(figsize=(20, 6))

        datafusion_vals = merged_df[f"{metric}_DF"]
        duckdb_vals     = merged_df[f"{metric}_DuckDB"]

        bars1 = ax.barh(y - width/2, datafusion_vals, height=width, label="DataFusion")
        bars2 = ax.barh(y + width/2, duckdb_vals,   height=width, label="DuckDB")

        ax.margins(x=0.005, y=0.01)
        ax.set_xlabel(metric)
        ax.set_ylabel("Query")
        ax.set_title(f"TPCH {metric}")
        ax.set_yticks(y)
        ax.set_yticklabels(merged_df["Query"])
        ax.legend()

        for bar in bars1 + bars2:
            value = bar.get_width()
            ax.annotate(f'{value:.2f}',
                        xy=(value, bar.get_y() + bar.get_height()/2),
                        xytext=(3, 0), textcoords="offset points",
                        ha='left', va='center', fontsize=8)

        plt.tight_layout()

        metric_filename = metric.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
        out_dir   = f"../results/plots/{prefix}"
        out_path  = f"{out_dir}/{metric_filename}.png"
        os.makedirs(out_dir, exist_ok=True)
        plt.savefig(out_path)
        plt.show()

else:
    def plot_metric(df, metric, title_suffix="", file_suffix=""):
        x = np.arange(len(df["Query"]))
        fig, ax = plt.subplots(figsize=(20, 6))

        datafusion_vals = df[f"{metric}_DF"]
        duckdb_vals     = df[f"{metric}_DuckDB"]

        bars1 = ax.bar(x - width/2, datafusion_vals, width, label="DataFusion")
        bars2 = ax.bar(x + width/2, duckdb_vals,   width, label="DuckDB")

        ax.margins(x=0.005)
        ax.set_xlabel("Query")
        ax.set_ylabel(metric)
        ax.set_title(f"TPCDS {metric}{title_suffix}")
        ax.set_xticks(x)
        ax.set_xticklabels(df["Query"], rotation=-90, ha="center")
        ax.legend()

        for bar in bars1 + bars2:
            h = bar.get_height()
            ax.annotate(f'{h:.2f}',
                        xy=(bar.get_x() + bar.get_width()/2, h),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=8)

        plt.tight_layout()

        metric_filename = metric.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
        out_dir  = f"../results/plots/{prefix}"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/{metric_filename}{file_suffix}.png"
        plt.savefig(out_path)
        plt.show()

    for metric in metrics:
        if metric == "Latency (s)":
            plot_metric(merged_df, metric)

            no_q72_df = merged_df[~merged_df["Query"].astype(str).eq("72")]
            plot_metric(no_q72_df, metric,
                        title_suffix=" (without Q72)",
                        file_suffix="_no_q72")
        else:
            plot_metric(merged_df, metric)
