#!/usr/bin/env bash

cd "$(dirname "$0")"

RESULTS_DIR="../results"
PLOTS_DIR="../results/plots"
if [ -d "$RESULTS_DIR" ]; then
  rm -rf "$RESULTS_DIR"/*
else
    mkdir $RESULTS_DIR
fi

mkdir $PLOTS_DIR
mkdir "$PLOTS_DIR"/tpch
mkdir "$PLOTS_DIR"/tpcds

python3 tpch_datafusion.py
python3 tpch_duckdb.py
python3 plots.py tpch

python3 tpcds_duckdb.py
python3 tpcds_datafusion.py
python3 plots.py tpcds