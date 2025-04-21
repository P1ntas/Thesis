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
df_duckdb = pd.read_csv(f"../results/{prefix}_duckdb.csv")

merged_df = pd.merge(df_datafusion, df_duckdb, on="Query", suffixes=("_DF", "_DuckDB"))

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
    
        datafusion_vals = merged_df.get(f"{metric}_DF")
        duckdb_vals = merged_df.get(f"{metric}_DuckDB")
    
        if datafusion_vals is None or duckdb_vals is None:
            print(f"Skipping metric '{metric}' because it is not in both DataFusion and DuckDB results.")
            plt.close(fig)
            continue

        bars1 = ax.barh(y - width/2, datafusion_vals, height=width, label="DataFusion")
        bars2 = ax.barh(y + width/2, duckdb_vals, height=width, label="DuckDB")

        ax.margins(x=0.005)
        ax.margins(y=0.01)
        
        ax.set_xlabel(metric)
        ax.set_ylabel("Query")
        ax.set_title(f"TPCH {metric}")
        ax.set_yticks(y)
        ax.set_yticklabels(merged_df["Query"])

        ax.legend()

        for bar in bars1 + bars2:
            value = bar.get_width()
            ax.annotate(
                f'{value:.2f}',
                xy=(value, bar.get_y() + bar.get_height() / 2),
                xytext=(3, 0),
                textcoords="offset points",
                ha='left',
                va='center',
                fontsize=8
            )

        plt.tight_layout()

        metric_filename = (metric.replace(" ", "_")
                                .replace("(", "")
                                .replace(")", "")
                                .replace("/", "_"))
        output_dir = f"../results/plots/{prefix}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/{metric_filename}.png"

        plt.savefig(output_path)
        plt.show()

else:
    x = np.arange(len(merged_df["Query"]))
    for metric in metrics:
        fig, ax = plt.subplots(figsize=(20, 6))
    
        datafusion_vals = merged_df.get(f"{metric}_DF")
        duckdb_vals = merged_df.get(f"{metric}_DuckDB")
    
        if datafusion_vals is None or duckdb_vals is None:
            print(f"Skipping metric '{metric}' because it is not in both DataFusion and DuckDB results.")
            plt.close(fig)
            continue

        bars1 = ax.bar(x - width / 2, datafusion_vals, width, label="DataFusion")
        bars2 = ax.bar(x + width / 2, duckdb_vals, width, label="DuckDB")

        ax.margins(x=0.005)
        ax.set_xlabel("Query")
        ax.set_ylabel(metric)
        ax.set_title(f"TPCDS {metric}")
        ax.set_xticks(x)
        ax.set_xticklabels(merged_df["Query"], rotation=-90, ha="center")

        if metric == "Latency (s)":
            ax.set_yscale('log')
            ax.set_ylabel(metric + " (log scale)")

        ax.legend()

        for bar in bars1 + bars2:
            height = bar.get_height()
            ax.annotate(
                f'{height:.2f}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha='center',
                va='bottom',
                fontsize=8
            )

        plt.tight_layout()

        metric_filename = (metric.replace(" ", "_")
                                .replace("(", "")
                                .replace(")", "")
                                .replace("/", "_"))
        output_dir = f"../results/plots/{prefix}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/{metric_filename}.png"

        plt.savefig(output_path)
        plt.show()
