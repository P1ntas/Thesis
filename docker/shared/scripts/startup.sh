#!/usr/bin/env bash

cd "$(dirname "$0")"

RESULTS_DIR="../results"
PLOTS_DIR="../results/plots"
RESULTS_ROARING_DIR="../results/roaring"
RESULTS_FAST_DIR="../results/fast"
RESULTS_KDTREE_DIR="../results/kdtree"
DATA_TPCH_DIR="../data/tpch/parquet/"


if [ -d "$RESULTS_DIR" ]; then
  rm -rf "$RESULTS_DIR"/*
else
    mkdir $RESULTS_DIR
fi

mkdir $RESULTS_ROARING_DIR
mkdir "$RESULTS_ROARING_DIR"/duckdb
mkdir "$RESULTS_ROARING_DIR"/datafusion
mkdir "$RESULTS_ROARING_DIR"/plots
mkdir $RESULTS_FAST_DIR
mkdir "$RESULTS_FAST_DIR"/duckdb
mkdir "$RESULTS_FAST_DIR"/datafusion
mkdir "$RESULTS_FAST_DIR"/plots
mkdir $RESULTS_KDTREE_DIR
mkdir "$RESULTS_KDTREE_DIR"/duckdb
mkdir "$RESULTS_KDTREE_DIR"/datafusion
mkdir "$RESULTS_KDTREE_DIR"/plots
mkdir $PLOTS_DIR
mkdir "$PLOTS_DIR"/tpch
mkdir "$PLOTS_DIR"/tpcds



make clean -C ./fast && make -C ./fast



make clean -C ./kdtree && make -C ./kdtree

DATA_TPCH_DIR="../data/tpch/parquet/"
if [ -d "$DATA_TPCH_DIR" ]; then
  rm -f "$DATA_TPCH_DIR"/filtered_*.parquet
fi
