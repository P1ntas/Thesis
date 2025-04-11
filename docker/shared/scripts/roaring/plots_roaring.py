import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

plots_dir = '../results/roaring/plots'
os.makedirs(plots_dir, exist_ok=True)

def create_bar_plot(plain_csv, roaring_csv, metric, title, output_filename):
    """
    Creates a horizontal bar plot with two bars per query:
    one for the plain results and one for the roaring results.
    """
    df_plain = pd.read_csv(plain_csv)
    df_roaring = pd.read_csv(roaring_csv)
    
    df_merged = pd.merge(df_plain, df_roaring, on='Query', suffixes=('_plain', '_roaring'))
    
    queries = df_merged['Query'].astype(str)
    plain_vals = df_merged[f"{metric}_plain"]
    roaring_vals = df_merged[f"{metric}_roaring"]
    
    y = np.arange(len(queries))
    height = 0.35  # This acts as the thickness for each horizontal bar.
    
    fig, ax = plt.subplots(figsize=(10, 6))
    # Create horizontal bars with an offset on the y-axis.
    rects1 = ax.barh(y - height/2, plain_vals, height, label='Plain')
    rects2 = ax.barh(y + height/2, roaring_vals, height, label='Roaring')
    
    # Set axis labels and title appropriate for horizontal bar plots.
    ax.set_ylabel('Query')
    ax.set_xlabel(metric)
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()

    # Annotate each bar with its value.
    for rect in rects1 + rects2:
        value = rect.get_width()  # For horizontal bars, width represents the metric value.
        ax.annotate(f'{value:.2f}',
                    xy=(value, rect.get_y() + rect.get_height()/2),
                    xytext=(3, 0),
                    textcoords="offset points",
                    ha='left', va='center')
    
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, output_filename))
    plt.close()

def create_bar_plot_sizes(csv_file, metric1, metric2, title, output_filename):
    """
    Creates a horizontal bar plot with two bars per query:
    one for 'Roaring Bitmap Size (MB)' and one for
    'Original Columns Size (MB)'.
    """
    df = pd.read_csv(csv_file)
    queries = df['Query'].astype(str)
    values1 = df[metric1]
    values2 = df[metric2]
    
    y = np.arange(len(queries))
    height = 0.35  # This acts as the thickness of each horizontal bar.
    
    fig, ax = plt.subplots(figsize=(10, 6))
    # Create horizontal bars with an offset on the y-axis.
    bars1 = ax.barh(y - height/2, values1, height, label=metric1)
    bars2 = ax.barh(y + height/2, values2, height, label=metric2)
    
    ax.set_ylabel('Query')
    ax.set_xlabel("Size (MB)")
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(queries)
    ax.legend()
    
    # Annotate each bar with its value.
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

# List of metrics for the plain vs roaring comparisons.
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

# Create horizontal bar plots for DuckDB results.
for metric in metrics:
    output_file = f"duckdb_{metric.replace(' ', '_').replace('(', '').replace(')', '').replace('%','pct').replace('/','_')}.png"
    title = f"DuckDB: {metric} Comparison (Plain vs Roaring)"
    create_bar_plot(duckdb_plain, duckdb_roaring, metric, title, output_file)

# Create horizontal bar plots for DataFusion results.
for metric in metrics:
    output_file = f"datafusion_{metric.replace(' ', '_').replace('(', '').replace(')', '').replace('%','pct').replace('/','_')}.png"
    title = f"DataFusion: {metric} Comparison (Plain vs Roaring)"
    create_bar_plot(datafusion_plain, datafusion_roaring, metric, title, output_file)

# Create a horizontal bar (column) plot that compares the two size metrics:
# "Roaring Bitmap Size (MB)" and "Original Columns Size (MB)".
create_bar_plot_sizes(
    csv_file=duckdb_roaring, 
    metric1='Roaring Bitmap Size (MB)', 
    metric2='Original Columns Size (MB)',
    title='Roaring vs Original Columns Size per Query',
    output_filename='roaring_vs_original_columns_size.png'
)
