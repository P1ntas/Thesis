#!/usr/bin/env bash

cd "$(dirname "$0")"

RESULTS_DIR="../results"
PLOTS_DIR="../results/plots"
RESULTS_ROARING_DIR="../results/roaring"
ROARING_DIR="./roaring"
DATA_TPCH_DIR="../data/tpch/parquet/"
DATA_TPCDS_DIR="../data/tpcds/parquet/"


if [ -d "$RESULTS_DIR" ]; then
  rm -rf "$RESULTS_DIR"/*
else
    mkdir $RESULTS_DIR
fi

mkdir $RESULTS_ROARING_DIR
mkdir "$RESULTS_ROARING_DIR"/duckdb
mkdir "$RESULTS_ROARING_DIR"/datafusion
mkdir "$RESULTS_ROARING_DIR"/plots
mkdir $PLOTS_DIR
mkdir "$PLOTS_DIR"/tpch
mkdir "$PLOTS_DIR"/tpcds

python3 tpch_datafusion.py
python3 tpch_duckdb.py
python3 plots.py tpch

python3 tpcds_duckdb.py
python3 tpcds_datafusion.py
python3 plots.py tpcds

for file in "$ROARING_DIR"/roaring_*.py; do
  python3 "$file"
done

if [ -d "$DATA_TPCH_DIR" ]; then
  rm -f "$DATA_TPCH_DIR"/filtered_*.parquet
fi

if [ -d "$DATA_TPCDS_DIR" ]; then
  rm -f "$DATA_TPCDS_DIR"/filtered_*.parquet
fi