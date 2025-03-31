# Tese

This repository ensures the contents of my thesis are easily reproducible to be verified by external entities. It provides benchmark results for two query benchmarking tools: **TPC-H** and **TPC-DS**. The results include plots for latency, memory usage, and IOPS.

## Setup

To run the benchmarks, execute the following command:

```bash
docker compose down && docker compose build && docker compose up -d
```

## Results

### TPC-H

![Latency](./docker/shared/results/plots/tpch/Latency_s.png)

![Peak Memory Usage](./docker/shared/results/plots/tpch/Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/plots/tpch/Average_Memory_Usage_MB.png)

![Input/Output Operations per Second](./docker/shared/results/plots/tpch/IOPS_ops_s.png)

### TPC-DS

![Latency](./docker/shared/results/plots/tpcds/Latency_s.png)

![Peak Memory Usage](./docker/shared/results/plots/tpcds/Peak_Memory_Usage_MB.png)

![Average Memory Usage](./docker/shared/results/plots/tpcds/Average_Memory_Usage_MB.png)

![Input/Output Operations per Second](./docker/shared/results/plots/tpcds/IOPS_ops_s.png)