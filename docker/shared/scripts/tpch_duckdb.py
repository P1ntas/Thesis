import duckdb
import os
from common import measure_query_execution, read_tpch_queries
from common import aggregate_benchmarks, write_csv_results

os.makedirs("../results", exist_ok=True)

con = duckdb.connect(':memory:')

with open('../data/tpch/parquet/schema.sql', 'r') as f:
    schema_sql = f.read()
con.execute(schema_sql)

with open('../data/tpch/parquet/load.sql', 'r') as f:
    load_sql = f.read()
con.execute(load_sql)

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