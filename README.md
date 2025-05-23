# Thesis

This repository ensures the contents of my thesis are easily reproducible to be verified by external entities. It provides benchmark results for two query benchmarking tools: **TPC-H** and **TPC-DS**. The results include plots for latency, IOPS, and memory and CPU usage, among other metrics.

## Project structure

- `./docker/shared/data/`: Contains generated dataset and queries based on the industry-standard **TPC-H** and **TPC-DS** benchmarking tools. The dataset files are in the `parquet` format, as these files are optimized for columnar storage and OLAP queries.

- `./docker/shared/results/`: Results of the benchmarks.

- `./docker/shared/scripts/`: This directory contains a suite of benchmarking scripts, including:

    - A startup script for launching the Docker container;
    - Scripts for executing benchmark queries;
    - Utilities for capturing and aggregating performance metrics;
    - Plotting scripts for visualizing the results.

    Additionally, it includes two subdirectories:

    - `roaring`: Contains benchmarking scripts specific to the Roaring Bitmap  ;
    - `fast`: Contains scripts tailored for benchmarking the FAST tree.

## Benchmark methodology

Integrating directly with the query engine would require a deep understanding of its internal workings and could potentially interfere with its optimized query planners. To avoid these complications, I adopted an alternative approach.

The scripts first build the index and then measure the performance of search and filter operations on the indexed data structure. Once filtering is complete, the resulting data is passed to the query engine for execution.

This method takes a conservative approach to benchmarking. Although it introduces some redundancy, the results are likely to underestimate real-world performance—meaning actual usage scenarios may yield better outcomes than those measured here.

## Setup

To run the benchmarks, execute the following command:

```bash
docker compose down && docker compose build && docker compose up -d
```

## Results

### Comparison between DuckDB and Apache DataFusion

#### TPC-H

![Latency](./docker/shared/results/plots/tpch/Latency_s.png)

![Peak Memory Usage](./docker/shared/results/plots/tpch/Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/plots/tpch/Average_Memory_Usage_MB.png)

![CPU Usage](./docker/shared/results/plots/tpch/CPU_Usage_%25.png)

![Input/Output Operations per Second](./docker/shared/results/plots/tpch/IOPS_ops_s.png)

#### TPC-DS

![Latency](./docker/shared/results/plots/tpcds/Latency_s.png)

![Peak Memory Usage](./docker/shared/results/plots/tpcds/Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/plots/tpcds/Average_Memory_Usage_MB.png)

![CPU Usage](./docker/shared/results/plots/tpcds/CPU_Usage_%25.png)

![Input/Output Operations per Second](./docker/shared/results/plots/tpcds/IOPS_ops_s.png)

### Roaring Bitmap

![Index Creation Time](./docker/shared/results/roaring/plots/bitmap_creation_time.png)

![Index Comparison Size](./docker/shared/results/roaring/plots/roaring_vs_original_columns_size.png)

#### DuckDB

![Latency](./docker/shared/results/roaring/plots/duckdb_Latency_s.png)

![Peak Memory Usage](./docker/shared/results/roaring/plots/duckdb_Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/roaring/plots/duckdb_Average_Memory_Usage_MB.png)

![CPU Usage](./docker/shared/results/roaring/plots/duckdb_CPU_Usage_pct.png)

![Input/Output Operations per Second](./docker/shared/results/roaring/plots/duckdb_IOPS_ops_s.png)

#### Apache DataFusion

![Latency](./docker/shared/results/roaring/plots/datafusion_Latency_s.png)

![Peak Memory Usage](./docker/shared/results/roaring/plots/datafusion_Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/roaring/plots/datafusion_Average_Memory_Usage_MB.png)

![CPU Usage](./docker/shared/results/roaring/plots/datafusion_CPU_Usage_pct.png)

![Input/Output Operations per Second](./docker/shared/results/roaring/plots/datafusion_IOPS_ops_s.png)

### FAST Tree

![Index Creation Time](./docker/shared/results/fast/plots/duckdb_fast_tree_creation_time.png)

![Index Comparison Size](./docker/shared/results/fast/plots/duckdb_fast_vs_original_columns_size.png)

#### DuckDB

![Latency](./docker/shared/results/fast/plots/duckdb_Latency_s.png)

![Peak Memory Usage](./docker/shared/results/fast/plots/duckdb_Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/fast/plots/duckdb_Average_Memory_Usage_MB.png)

![CPU Usage](./docker/shared/results/fast/plots/duckdb_CPU_Usage_pct.png)

![Input/Output Operations per Second](./docker/shared/results/fast/plots/duckdb_IOPS_ops_s.png)

#### Apache DataFusion

![Latency](./docker/shared/results/fast/plots/datafusion_Latency_s.png)

![Peak Memory Usage](./docker/shared/results/fast/plots/datafusion_Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/fast/plots/datafusion_Average_Memory_Usage_MB.png)

![CPU Usage](./docker/shared/results/fast/plots/datafusion_CPU_Usage_pct.png)

![Input/Output Operations per Second](./docker/shared/results/fast/plots/datafusion_IOPS_ops_s.png)

### Kd Tree

![Index Creation Time](./docker/shared/results/kdtree/plots/kd_tree_creation_time.png)

![Index Comparison Size](./docker/shared/results/kdtree/plots/tree_size_duckdb.png)

#### DuckDB

![Latency](./docker/shared/results/kdtree/plots/latency_duckdb.png)

![Peak Memory Usage](./docker/shared/results/kdtree/plots/peak_mem_duckdb.png)

![Average Memory Usage](./docker/shared/results/kdtree/plots/avg_mem_duckdb.png)

![CPU Usage](./docker/shared/results/kdtree/plots/cpu_usage_duckdb.png)

![Input/Output Operations per Second](./docker/shared/results/kdtree/plots/iops_duckdb.png)

#### Apache DataFusion

![Latency](./docker/shared/results/kdtree/plots/latency_datafusion.png)

![Peak Memory Usage](./docker/shared/results/kdtree/plots/peak_mem_datafusion.png)

![Average Memory Usage](./docker/shared/results/kdtree/plots/avg_mem_datafusion.png)

![CPU Usage](./docker/shared/results/kdtree/plots/cpu_usage_datafusion.png)

![Input/Output Operations per Second](./docker/shared/results/kdtree/plots/iops_datafusion.png)
