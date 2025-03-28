import duckdb
import time
import psutil
import os
import threading
import csv

os.makedirs("../results", exist_ok=True)

con = duckdb.connect(':memory:')

con.execute('''
CREATE OR REPLACE TABLE nation_renamed (
    n_nationkey  INTEGER NOT NULL,
    n_name       CHAR(25) NOT NULL,
    n_regionkey  INTEGER NOT NULL,
    n_comment    VARCHAR(152),
    n_none       VARCHAR(152)
);
''')
con.execute('''
INSERT INTO nation_renamed (n_nationkey, n_name, n_regionkey, n_comment, n_none)
SELECT * FROM '../data/tpch/parquet/nation.parquet';
''')

con.execute('''
CREATE OR REPLACE TABLE customer_renamed (
    c_custkey    INTEGER NOT NULL,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_nationkey  INTEGER NOT NULL,
    c_phone      VARCHAR(15),
    c_acctbal    DECIMAL(15,2),
    c_mktsegment VARCHAR(10),
    c_comment    VARCHAR(117),
    c_none       VARCHAR(152)
);
''')
con.execute('''
INSERT INTO customer_renamed (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, c_none)
SELECT * FROM '../data/tpch/parquet/customer.parquet';
''')

con.execute('''
CREATE OR REPLACE TABLE orders_renamed (
    o_orderkey     INTEGER NOT NULL,
    o_custkey      INTEGER NOT NULL,
    o_orderstatus  CHAR(1) NOT NULL,
    o_totalprice   DECIMAL(15,2) NOT NULL,
    o_orderdate    DATE NOT NULL,
    o_orderpriority VARCHAR(15),
    o_clerk        VARCHAR(15),
    o_shippriority INTEGER,
    o_comment      VARCHAR(79),
    o_none         VARCHAR(152)
);
''')
con.execute('''
INSERT INTO orders_renamed (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_none)
SELECT * FROM '../data/tpch/parquet/orders.parquet';
''')

con.execute('''
CREATE OR REPLACE TABLE part_renamed (
    p_partkey     INTEGER NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INTEGER NOT NULL,
    p_container   VARCHAR(10),
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment     VARCHAR(23),
    p_none        VARCHAR(152)
);
''')
con.execute('''
INSERT INTO part_renamed (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, p_none)
SELECT * FROM '../data/tpch/parquet/part.parquet';
''')

con.execute('''
CREATE OR REPLACE TABLE partsupp_renamed (
    ps_partkey    INTEGER NOT NULL,
    ps_suppkey    INTEGER NOT NULL,
    ps_availqty   INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment    VARCHAR(199),
    ps_none       VARCHAR(152)
);
''')
con.execute('''
INSERT INTO partsupp_renamed (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, ps_none)
SELECT * FROM '../data/tpch/parquet/partsupp.parquet';
''')

con.execute('''
CREATE OR REPLACE TABLE region_renamed (
    r_regionkey INTEGER NOT NULL,
    r_name      VARCHAR(25) NOT NULL,
    r_comment   VARCHAR(152),
    r_none      VARCHAR(152)
);
''')
con.execute('''
INSERT INTO region_renamed (r_regionkey, r_name, r_comment, r_none)
SELECT * FROM '../data/tpch/parquet/region.parquet';
''')

con.execute('''
CREATE OR REPLACE TABLE supplier_renamed (
    s_suppkey  INTEGER NOT NULL,
    s_name     VARCHAR(25) NOT NULL,
    s_address  VARCHAR(40) NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone    VARCHAR(15),
    s_acctbal  DECIMAL(15,2),
    s_comment  VARCHAR(101),
    s_none     VARCHAR(152)
);
''')
con.execute('''
INSERT INTO supplier_renamed (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, s_none)
SELECT * FROM '../data/tpch/parquet/supplier.parquet';
''')

con.execute('''
CREATE OR REPLACE TABLE lineitem_renamed (
    l_orderkey      INTEGER NOT NULL,
    l_partkey       INTEGER NOT NULL,
    l_suppkey       INTEGER NOT NULL,
    l_linenumber    INTEGER NOT NULL,
    l_quantity      DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount      DECIMAL(15,2) NOT NULL,
    l_tax           DECIMAL(15,2) NOT NULL,
    l_returnflag    CHAR(1),
    l_linestatus    CHAR(1),
    l_shipdate      DATE,
    l_commitdate    DATE,
    l_receiptdate   DATE,
    l_shipinstruct  VARCHAR(25),
    l_shipmode      VARCHAR(10),
    l_comment       VARCHAR(44),
    l_none          VARCHAR(152)
);
''')
con.execute('''
INSERT INTO lineitem_renamed (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, l_none)
SELECT * FROM '../data/tpch/parquet/lineitem.parquet';
''')

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
        con.execute(query_str1).fetchall()
        result_batches = con.execute(query_str2).fetchall()
        con.execute(query_str3).fetchall()
    else:
        result_batches = con.execute(query_str).fetchall()

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

csv_output_path = "../results/tpch_duckdb.csv"
fieldnames = ["Query", "Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]
with open(csv_output_path, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
        writer.writerow(row)
