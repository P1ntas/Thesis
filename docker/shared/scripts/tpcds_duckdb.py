import duckdb
import os
from common import measure_query_execution, aggregate_benchmarks, write_csv_results

os.makedirs("../results", exist_ok=True)

con = duckdb.connect(':memory:')

with open('../data/tpcds/parquet/schema.sql', 'r') as f:
    schema_sql = f.read()
con.execute(schema_sql)

with open('../data/tpcds/parquet/load.sql', 'r') as f:
    load_sql = f.read()
con.execute(load_sql)

with open('../data/tpcds/queries/query_0.sql', 'r') as f:
    queries_text = f.read()
queries = [q.strip() for q in queries_text.strip().split("\n\n\n") if q.strip()]

def measure_query(query: str):
    def exec_fn():
        return con.execute(query).fetchall()
    return measure_query_execution(exec_fn)

results = aggregate_benchmarks(queries, runs=3, measure_fn=measure_query, error_check=True)

csv_output_path = "../results/tpcds_duckdb.csv"
fieldnames = ["Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]
write_csv_results(csv_output_path, fieldnames, results)
