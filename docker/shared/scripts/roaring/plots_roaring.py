import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

plots_dir = '../results/roaring/plots'
os.makedirs(plots_dir, exist_ok=True)

def create_bar_plot(plain_csv, roaring_csv, metric, title, output_filename):
    df_plain = pd.read_csv(plain_csv)
    df_roaring = pd.read_csv(roaring_csv)
    
    df_merged = pd.merge(df_plain, df_roaring, on='Query', suffixes=('_plain', '_roaring'))
    
    queries = df_merged['Query'].astype(str)
    plain_vals = df_merged[f"{metric}_plain"]
    roaring_vals = df_merged[f"{metric}_roaring"]
    
    x = np.arange(len(queries))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.bar(x - width/2, plain_vals, width, label='Plain')
    rects2 = ax.bar(x + width/2, roaring_vals, width, label='Roaring')
    
    ax.set_xlabel('Query')
    ax.set_ylabel(metric)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(queries, rotation=45, ha='right')
    ax.legend()

    for rect in rects1 + rects2:
        height = rect.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

def create_line_plot(roaring_csv, metric, title, output_filename):
    df = pd.read_csv(roaring_csv)
    queries = df['Query'].astype(str)
    metric_vals = df[metric]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(queries, metric_vals, marker='o', linestyle='-')
    
    ax.set_xlabel('Query')
    ax.set_ylabel(metric)
    ax.set_title(title)
    ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

metrics = [
    "Latency (s)",
    "CPU Usage (%)",
    "Peak Memory Usage (MB)",
    "Average Memory Usage (MB)",
    "IOPS (ops/s)"
]

duckdb_plain = '../results/tpch_duckdb.csv'
duckdb_roaring = '../results/roaring/duckdb/roaring_tpch.csv'
datafusion_plain = '../results/tpch_datafusion.csv'
datafusion_roaring = '../results/roaring/datafusion/roaring_tpch.csv'

for metric in metrics:
    output_file = f"duckdb_{metric.replace(' ', '_').replace('(', '').replace(')', '').replace('%','pct').replace('/','_')}.png"
    title = f"DuckDB: {metric} Comparison (Plain vs Roaring)"
    create_bar_plot(duckdb_plain, duckdb_roaring, metric, title, output_file)

for metric in metrics:
    output_file = f"datafusion_{metric.replace(' ', '_').replace('(', '').replace(')', '').replace('%','pct').replace('/','_')}.png"
    title = f"DataFusion: {metric} Comparison (Plain vs Roaring)"
    create_bar_plot(datafusion_plain, datafusion_roaring, metric, title, output_file)

create_line_plot(
    roaring_csv=duckdb_roaring,
    metric='Roaring Bitmap Size (MB)',
    title='Roaring Bitmap Size per Query',
    output_filename='roaring_bitmap_size.png'
)
