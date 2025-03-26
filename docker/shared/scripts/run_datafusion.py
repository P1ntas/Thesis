from datafusion import SessionContext
import time
import psutil
import os
import threading
import csv

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
        file_path1 = f"../data/tpch/queries/{query_number}a.sql"
        file_path2 = f"../data/tpch/queries/{query_number}b.sql"
        file_path3 = f"../data/tpch/queries/{query_number}c.sql"
        with open(file_path1, "r") as file:
            query_str1 = file.read()
        with open(file_path2, "r") as file:
            query_str2 = file.read()
        with open(file_path3, "r") as file:
            query_str3 = file.read()
    else:
        file_path = f"../data/tpch/queries/{query_number}.sql"
        with open(file_path, "r") as file:
            query_str = file.read()

    process = psutil.Process(os.getpid())
    memory_samples = []
    stop_event = threading.Event()

    def sample_memory():
        while not stop_event.is_set():
            try:
                mem = process.memory_info().rss
                memory_samples.append(mem)
            except Exception as e:
                print("Memory sampling error:", e)
            time.sleep(0.1)

    mem_thread = threading.Thread(target=sample_memory)
    mem_thread.start()
    io_before = process.io_counters()
    start_time = time.perf_counter()

    if query_number == 15:
        query1 = ctx.sql(query_str1)
        query2 = ctx.sql(query_str2)
        query3 = ctx.sql(query_str3)
        query1.collect()
        result_batches = query2.collect()
        query3.collect()
    else:
        query = ctx.sql(query_str)
        result_batches = query.collect()

    end_time = time.perf_counter()
    io_after = process.io_counters()
    stop_event.set()
    mem_thread.join()

    latency = end_time - start_time
    if memory_samples:
        peak_memory = max(memory_samples)
        avg_memory = sum(memory_samples) / len(memory_samples)
    else:
        peak_memory = avg_memory = 0

    read_count = io_after.read_count - io_before.read_count
    write_count = io_after.write_count - io_before.write_count
    total_iops = (read_count + write_count) / latency if latency > 0 else 0

    peak_memory_mb = peak_memory / (1024 * 1024)
    avg_memory_mb = avg_memory / (1024 * 1024)

    return {
        "Query": query_number,
        "Latency (s)": latency,
        "Peak Memory Usage (MB)": peak_memory_mb,
        "Average Memory Usage (MB)": avg_memory_mb,
        "IOPS (ops/s)": total_iops
    }

results = []
runs = 3
for i in range(1, 23):
    sum_latency = 0.0
    sum_peak_memory = 0.0
    sum_avg_memory = 0.0
    sum_iops = 0.0
    for _ in range(runs):
        metrics = measure_query(i)
        sum_latency += metrics["Latency (s)"]
        sum_peak_memory += metrics["Peak Memory Usage (MB)"]
        sum_avg_memory += metrics["Average Memory Usage (MB)"]
        sum_iops += metrics["IOPS (ops/s)"]
    avg_metrics = {
        "Query": i,
        "Latency (s)": sum_latency / runs,
        "Peak Memory Usage (MB)": sum_peak_memory / runs,
        "Average Memory Usage (MB)": sum_avg_memory / runs,
        "IOPS (ops/s)": sum_iops / runs
    }
    results.append(avg_metrics)

csv_output_path = "../results/tpch.csv"
fieldnames = ["Query", "Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]
with open(csv_output_path, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
        writer.writerow(row)
