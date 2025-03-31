from datafusion import SessionContext
import os
from common import measure_query_execution, read_tpch_queries
from common import aggregate_benchmarks, write_csv_results

os.makedirs("../results", exist_ok=True)

ctx = SessionContext()
ctx.register_parquet('lineitem', '../data/tpch/parquet/lineitem.parquet')
ctx.register_parquet('customer', '../data/tpch/parquet/customer.parquet')
ctx.register_parquet('nation', '../data/tpch/parquet/nation.parquet')
ctx.register_parquet('orders', '../data/tpch/parquet/orders.parquet')
ctx.register_parquet('part', '../data/tpch/parquet/part.parquet')
ctx.register_parquet('partsupp', '../data/tpch/parquet/partsupp.parquet')
ctx.register_parquet('region', '../data/tpch/parquet/region.parquet')
ctx.register_parquet('supplier', '../data/tpch/parquet/supplier.parquet')

ctx.sql("""CREATE VIEW lineitem_renamed AS
    SELECT
      column00 AS l_orderkey,
      column01 AS l_partkey,
      column02 AS l_suppkey,
      column03 AS l_linenumber,
      column04 AS l_quantity,
      column05 AS l_extendedprice,
      column06 AS l_discount,
      column07 AS l_tax,
      column08 AS l_returnflag,
      column09 AS l_linestatus,
      column10 AS l_shipdate,
      column11 AS l_commitdate,
      column12 AS l_receiptdate,
      column13 AS l_shipinstruct,
      column14 AS l_shipmode,
      column15 AS l_comment,
      column16 AS l_none
    FROM lineitem;""")

ctx.sql("""CREATE VIEW part_renamed AS
    SELECT
      column0 AS p_partkey,
      column1 AS p_name,
      column2 AS p_mfgr,
      column3 AS p_brand,
      column4 AS p_type,
      column5 AS p_size,
      column6 AS p_container,
      column7 AS p_retailprice,
      column8 AS p_comment,
      column9 AS p_none
    FROM part;""")

ctx.sql("""CREATE VIEW supplier_renamed AS
    SELECT
      column0 AS s_suppkey,
      column1 AS s_name,
      column2 AS s_address,
      column3 AS s_nationkey,
      column4 AS s_phone,
      column5 AS s_acctbal,
      column6 AS s_comment,
      column7 AS s_none
    FROM supplier;""")

ctx.sql("""CREATE VIEW partsupp_renamed AS
    SELECT
      column0 AS ps_partkey,
      column1 AS ps_suppkey,
      column2 AS ps_availqty,
      column3 AS ps_supplycost,
      column4 AS ps_comment,
      column5 AS ps_none
    FROM partsupp;""")

ctx.sql("""CREATE VIEW nation_renamed AS
    SELECT
      column0 AS n_nationkey,
      column1 AS n_name,
      column2 AS n_regionkey,
      column3 AS n_comment,
      column4 AS n_none
    FROM nation;""")

ctx.sql("""CREATE VIEW region_renamed AS
    SELECT
      column0 AS r_regionkey,
      column1 AS r_name,
      column2 AS r_comment,
      column3 AS r_none
    FROM region;""")

ctx.sql("""CREATE VIEW orders_renamed AS
    SELECT
      column0 AS o_orderkey,
      column1 AS o_custkey,
      column2 AS o_orderstatus,
      column3 AS o_totalprice,
      column4 AS o_orderdate,
      column5 AS o_orderpriority,
      column6 AS o_clerk,
      column7 AS o_shippriority,
      column8 AS o_comment,
      column9 AS o_none
    FROM orders;""")

ctx.sql("""CREATE VIEW customer_renamed AS
    SELECT
      column0 AS c_custkey,
      column1 AS c_name,
      column2 AS c_address,
      column3 AS c_nationkey,
      column4 AS c_phone,
      column5 AS c_acctbal,
      column6 AS c_mktsegment,
      column7 AS c_comment,
      column8 AS c_none
    FROM customer;""")

def measure_query(query_number: int):
    if query_number == 15:
        query_strs = read_tpch_queries(query_number)
        query1 = ctx.sql(query_strs[0])
        query2 = ctx.sql(query_strs[1])
        query3 = ctx.sql(query_strs[2])
        def exec_fn():
            query1.collect()
            result_batches = query2.collect()
            query3.collect()
            return result_batches
    else:
        query_str = read_tpch_queries(query_number)
        query = ctx.sql(query_str)
        def exec_fn():
            return query.collect()
    result = measure_query_execution(exec_fn)
    result["Query"] = query_number
    return result

queries = list(range(1, 23))

results = aggregate_benchmarks(queries, runs=3, measure_fn=measure_query, error_check=False)

csv_output_path = "../results/tpch_datafusion.csv"
fieldnames = ["Query", "Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]
write_csv_results(csv_output_path, fieldnames, results)
