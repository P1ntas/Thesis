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

metrics = ["Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]

width = 0.35  
x = np.arange(len(merged_df["Query"])) 

for metric in metrics:
    fig, ax = plt.subplots(figsize=(10, 6))
    
    datafusion_vals = merged_df[f"{metric}_DF"]
    duckdb_vals = merged_df[f"{metric}_DuckDB"]
    
    bars1 = ax.bar(x - width/2, datafusion_vals, width, label="DataFusion")
    bars2 = ax.bar(x + width/2, duckdb_vals, width, label="DuckDB")
    
    ax.set_xlabel("Query")
    ax.set_ylabel(metric)
    ax.set_title(f"{prefix.upper()} {metric}")
    ax.set_xticks(x)
    ax.set_xticklabels(merged_df["Query"])
    ax.legend()
    
    for bar in bars1 + bars2:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    
    metric_filename = metric.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
    output_path = f"../results/plots/{prefix}/{metric_filename}.png"
    plt.savefig(output_path)
    plt.show()
