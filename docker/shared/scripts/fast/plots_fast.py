import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

plots_dir = '../results/fast/plots'
os.makedirs(plots_dir, exist_ok=True)

def create_bar_plot(plain_csv, fast_csv, metric, title, output_filename):
    df_plain = pd.read_csv(plain_csv)
    df_fast = pd.read_csv(fast_csv)
    df_merged = pd.merge(df_plain, df_fast, on='Query', suffixes=('_plain', '_fast'))

    queries = df_merged['Query'].astype(str)
    plain_vals = df_merged[f"{metric}_plain"]
    fast_vals = df_merged[f"{metric}_fast"]

    y = np.arange(len(queries))
    height = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.barh(y - height/2, plain_vals, height, label='Plain')
    rects2 = ax.barh(y + height/2, fast_vals, height, label='Fast Tree')

    ax.set_ylabel('Query')
    ax.set_xlabel(metric)
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.margins(y=0.03)
    ax.legend()

    for rect in list(rects1) + list(rects2):
        value = rect.get_width()
        ax.annotate(f'{value:.2f}',
                    xy=(value, rect.get_y() + rect.get_height()/2),
                    xytext=(3, 0), textcoords="offset points",
                    ha='left', va='center')

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

def create_duckdb_size_plot(duckdb_csv, metric1, metric2, title, output_filename):
    df = pd.read_csv(duckdb_csv)[['Query', metric1, metric2]]
    df = df.rename(columns={metric1: 'Fast Tree Size', metric2: 'Original Column Size'})
    queries = df['Query'].astype(str)
    y = np.arange(len(queries))
    height = 0.35

    fig, ax = plt.subplots(figsize=(12, 7))
    rects1 = ax.barh(y - height/2, df['Fast Tree Size'], height, label='Fast Tree')
    rects2 = ax.barh(y + height/2, df['Original Column Size'], height, label='Original Column')

    ax.set_ylabel('Query')
    ax.set_xlabel('Size (MB)')
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()

    for rects in [rects1, rects2]:
        for rect in rects:
            value = rect.get_width()
            ax.annotate(f'{value:.2f}',
                        xy=(value, rect.get_y() + rect.get_height()/2),
                        xytext=(3, 0), textcoords="offset points",
                        ha='left', va='center')

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

def create_duckdb_creation_time_plot(duckdb_csv, metric, title, output_filename):
    df = pd.read_csv(duckdb_csv)[['Query', metric]].rename(
        columns={metric: 'Fast Tree Creation Time'})
    queries = df['Query'].astype(str)
    y = np.arange(len(queries))
    height = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    rects = ax.barh(y, df['Fast Tree Creation Time'], height, label='Fast Tree')

    ax.set_ylabel('Query')
    ax.set_xlabel(metric)
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()

    for rect in rects:
        value = rect.get_width()
        ax.annotate(f'{value:.2f}',
                    xy=(value, rect.get_y() + rect.get_height()/2),
                    xytext=(3, 0), textcoords="offset points",
                    ha='left', va='center')

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

duckdb_plain      = '../results/tpch_duckdb.csv'
duckdb_fast       = '../results/fast/duckdb/fast_tpch.csv'
datafusion_plain  = '../results/tpch_datafusion.csv'
datafusion_fast   = '../results/fast/datafusion/fast_tpch.csv'

deep_metrics = [
    "Latency (s)",
    "CPU Usage (%)",
    "Peak Memory Usage (MB)",
    "Average Memory Usage (MB)",
    "IOPS (ops/s)"
]

for metric in deep_metrics:
    fname = metric.replace(' ', '_').replace('(', '').replace(')', '') \
                   .replace('%','pct').replace('/','_')
    create_bar_plot(
        duckdb_plain,
        duckdb_fast,
        metric,
        f"DuckDB: {metric} Comparison (Plain vs Fast Tree)",
        f"duckdb_{fname}.png"
    )
    create_bar_plot(
        datafusion_plain,
        datafusion_fast,
        metric,
        f"DataFusion: {metric} Comparison (Plain vs Fast Tree)",
        f"datafusion_{fname}.png"
    )

create_duckdb_size_plot(
    duckdb_fast,
    metric1='Fast Tree Size (MB)',
    metric2='Original Column Size (MB)',
    title='Fast Tree vs Original Columns Size per Query',
    output_filename='fast_vs_original_columns_size.png'
)

create_duckdb_creation_time_plot(
    duckdb_fast,
    metric='Fast Tree Creation Time (s)',
    title='Fast Tree Creation Time per Query',
    output_filename='fast_tree_creation_time.png'
)
