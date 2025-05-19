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
    ax.margins(y=0.02)
    ax.set_yticklabels(queries)
    ax.legend()

    for rect in rects1 + rects2:
        value = rect.get_width()
        ax.annotate(f'{value:.2f}',
                    xy=(value, rect.get_y() + rect.get_height()/2),
                    xytext=(3, 0), textcoords="offset points",
                    ha='left', va='center')

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

def create_combined_size_plot(duckdb_csv, datafusion_csv, metric1, metric2, title, output_filename):
    df_dd = pd.read_csv(duckdb_csv)[['Query', metric1, metric2]]
    df_df = pd.read_csv(datafusion_csv)[['Query', metric1, metric2]]

    df_dd = df_dd.rename(columns={metric1: f"{metric1}_DuckDB",
                                  metric2: f"{metric2}_DuckDB"})
    df_df = df_df.rename(columns={metric1: f"{metric1}_DataFusion",
                                  metric2: f"{metric2}_DataFusion"})
    df = pd.merge(df_dd, df_df, on='Query')

    queries = df['Query'].astype(str)
    y = np.arange(len(queries))
    series = [
        (f"{metric1}_DuckDB", 'DuckDB Fast Tree Size'),
        (f"{metric2}_DuckDB", 'DuckDB Original Column Size'),
        (f"{metric1}_DataFusion", 'DataFusion Fast Tree Size'),
        (f"{metric2}_DataFusion", 'DataFusion Original Column Size'),
    ]
    n = len(series)
    height = 0.8 / n
    offsets = [(i - (n-1)/2) * height for i in range(n)]

    fig, ax = plt.subplots(figsize=(12, 7))
    for (col, label), offset in zip(series, offsets):
        bars = ax.barh(y + offset, df[col], height, label=label)
        for bar in bars:
            value = bar.get_width()
            ax.annotate(f'{value:.2f}',
                        xy=(value, bar.get_y() + bar.get_height()/2),
                        xytext=(3, 0), textcoords="offset points",
                        ha='left', va='center')

    ax.set_ylabel('Query')
    ax.set_xlabel('Size (MB)')
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

def create_combined_creation_time_plot(duckdb_csv, datafusion_csv, metric, title, output_filename):
    df_dd = pd.read_csv(duckdb_csv)[['Query', metric]].rename(
        columns={metric: 'DuckDB'})
    df_df = pd.read_csv(datafusion_csv)[['Query', metric]].rename(
        columns={metric: 'DataFusion'})
    df = pd.merge(df_dd, df_df, on='Query')

    queries = df['Query'].astype(str)
    y = np.arange(len(queries))
    height = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.barh(y - height/2, df['DuckDB'], height, label='DuckDB')
    rects2 = ax.barh(y + height/2, df['DataFusion'], height, label='DataFusion')

    ax.set_ylabel('Query')
    ax.set_xlabel(metric)
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()

    for rect in rects1 + rects2:
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

metrics = [
    "Latency (s)",
    "CPU Usage (%)",
    "Peak Memory Usage (MB)",
    "Average Memory Usage (MB)",
    "IOPS (ops/s)"
]

for metric in metrics:
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

create_combined_size_plot(
    duckdb_fast,
    datafusion_fast,
    metric1='Fast Tree Size (MB)',
    metric2='Original Column Size (MB)',
    title='Fast Tree vs Original Columns Size per Query (DuckDB & DataFusion)',
    output_filename='fast_vs_original_columns_size.png'
)

create_combined_creation_time_plot(
    duckdb_fast,
    datafusion_fast,
    metric='Fast Tree Creation Time (s)',
    title='Fast Tree Creation Time per Query (DuckDB & DataFusion)',
    output_filename='fast_tree_creation_time.png'
)
