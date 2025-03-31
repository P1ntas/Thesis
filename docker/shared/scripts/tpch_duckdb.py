import duckdb
import os
from common import measure_query_execution, read_tpch_queries
from common import aggregate_benchmarks, write_csv_results

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
        query_strs = read_tpch_queries(query_number)
        def exec_fn():
            con.execute(query_strs[0]).fetchall()
            result_batches = con.execute(query_strs[1]).fetchall()
            con.execute(query_strs[2]).fetchall()
            return result_batches
    else:
        query_str = read_tpch_queries(query_number)
        def exec_fn():
            return con.execute(query_str).fetchall()
    result = measure_query_execution(exec_fn)
    result["Query"] = query_number
    return result

queries = list(range(1, 23))

results = aggregate_benchmarks(queries, runs=3, measure_fn=measure_query, error_check=False)

csv_output_path = "../results/tpch_duckdb.csv"  
fieldnames = ["Query", "Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]
write_csv_results(csv_output_path, fieldnames, results)
