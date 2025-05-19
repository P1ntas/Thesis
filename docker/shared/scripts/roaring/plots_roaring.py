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
    
    y = np.arange(len(queries))
    height = 0.35 
    
    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.barh(y - height/2, plain_vals, height, label='Plain')
    rects2 = ax.barh(y + height/2, roaring_vals, height, label='Roaring')
    
    ax.set_ylabel('Query')
    ax.set_xlabel(metric)
    ax.set_title(title)
    ax.margins(y=0.03)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()

    for rect in rects1 + rects2:
        value = rect.get_width() 
        ax.annotate(f'{value:.2f}',
                    xy=(value, rect.get_y() + rect.get_height()/2),
                    xytext=(3, 0),
                    textcoords="offset points",
                    ha='left', va='center')
    
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

def create_bar_plot_sizes(csv_file, metric1, metric2, title, output_filename):
    df = pd.read_csv(csv_file)
    queries = df['Query'].astype(str)
    values1 = df[metric1]
    values2 = df[metric2]
    
    y = np.arange(len(queries))
    height = 0.35 
    
    fig, ax = plt.subplots(figsize=(10, 6))
    bars1 = ax.barh(y - height/2, values1, height, label=metric1)
    bars2 = ax.barh(y + height/2, values2, height, label=metric2)
    
    ax.set_ylabel('Query')
    ax.set_xlabel("Size (MB)")
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()
    
    for bar in bars1 + bars2:
        value = bar.get_width()
        ax.annotate(f'{value:.2f}',
                    xy=(value, bar.get_y() + bar.get_height()/2),
                    xytext=(3, 0),
                    textcoords="offset points",
                    ha='left', va='center')
    
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

create_bar_plot_sizes(
    csv_file=duckdb_roaring, 
    metric1='Roaring Bitmap Size (MB)', 
    metric2='Original Columns Size (MB)',
    title='Roaring vs Original Columns Size per Query',
    output_filename='roaring_vs_original_columns_size.png'
)

def create_single_bar_plot(csv_file, metric, title, output_filename):
    df = pd.read_csv(csv_file)
    queries = df['Query'].astype(str)
    values = df[metric]

    y = np.arange(len(queries))
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(y, values, height=0.6)

    ax.set_ylabel('Query')
    ax.set_xlabel(metric)
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)

    for bar in bars:
        value = bar.get_width()
        ax.annotate(f'{value:.2f}',
                    xy=(value, bar.get_y() + bar.get_height()/2),
                    xytext=(3, 0),
                    textcoords="offset points",
                    ha='left', va='center')

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

create_single_bar_plot(
    csv_file=duckdb_roaring,
    metric='Bitmap Creation Time (s)',
    title='DuckDB Roaring: Bitmap Creation Time per Query',
    output_filename='bitmap_creation_time.png'
)
