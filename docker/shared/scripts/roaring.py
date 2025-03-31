import pandas as pd
from pyroaring import BitMap

customer_renamed = pd.read_parquet('../data/tpch/parquet/customer.parquet')
lineitem_renamed = pd.read_parquet('../data/tpch/parquet/lineitem.parquet')
nation_renamed = pd.read_parquet('../data/tpch/parquet/nation.parquet')
orders_renamed = pd.read_parquet('../data/tpch/parquet/orders.parquet')
part_renamed = pd.read_parquet('../data/tpch/parquet/part.parquet')
partsupp_renamed = pd.read_parquet('../data/tpch/parquet/partsupp.parquet')
region_renamed = pd.read_parquet('../data/tpch/parquet/region.parquet')
supplier_renamed = pd.read_parquet('../data/tpch/parquet/supplier.parquet')

customer_renamed.columns = ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment", "c_none"]
lineitem_renamed.columns = ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment, l_none"]
nation_renamed.columns = ["n_nationkey", "n_name", "n_regionkey", "n_comment", "n_none"]
orders_renamed.columns = ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment", "o_none"]
part_renamed.columns = ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment", "p_none"]
partsupp_renamed.columns = ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment", "ps_none"]
region_renamed.columns = ["r_regionkey", "r_name", "r_comment", "r_none"]
supplier_renamed.columns = ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment", "s_none"]

