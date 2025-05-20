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

python3 tpch_datafusion.py
python3 tpch_duckdb.py
python3 plots.py tpch

python3 tpcds_duckdb.py
python3 tpcds_datafusion.py
python3 plots.py tpcds

python3 ./roaring/roaring_1.py
python3 ./roaring/roaring_3.py
python3 ./roaring/roaring_4.py
python3 ./roaring/roaring_5.py
python3 ./roaring/roaring_6.py
python3 ./roaring/roaring_8.py
python3 ./roaring/roaring_10.py
python3 ./roaring/roaring_12.py
python3 ./roaring/roaring_22.py
python3 ./roaring/plots_roaring.py

make clean -C ./fast && make -C ./fast

python3 ./fast/fast_1.py
python3 ./fast/fast_3.py
python3 ./fast/fast_4.py
python3 ./fast/fast_5.py
python3 ./fast/fast_6.py
python3 ./fast/fast_7.py
python3 ./fast/fast_8.py
python3 ./fast/fast_10.py
python3 ./fast/fast_12.py
python3 ./fast/fast_14.py
python3 ./fast/fast_15.py
python3 ./fast/fast_19.py
python3 ./fast/fast_20.py
python3 ./fast/plots_fast.py

make clean -C ./kdtree && make -C ./kdtree

python3 ./kdtree/kdtree_6.py
python3 ./kdtree/kdtree_19.py
python3 ./kdtree/plots_kdtree.py 

DATA_TPCH_DIR="../data/tpch/parquet/"
if [ -d "$DATA_TPCH_DIR" ]; then
  rm -f "$DATA_TPCH_DIR"/filtered_*.parquet
fi
