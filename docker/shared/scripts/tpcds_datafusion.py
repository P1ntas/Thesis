from datafusion import SessionContext
import os
import re
from common import measure_query_execution, aggregate_benchmarks, write_csv_results, default_special_handler

os.makedirs("../results", exist_ok=True)

ctx = SessionContext()

with open('../data/tpcds/parquet/load.sql', 'r') as load_file:
    load_sql = load_file.read()

load_statements = [stmt.strip() for stmt in load_sql.split(';') if stmt.strip()]
for stmt in load_statements:
    m = re.search(r"COPY\s+(\S+)\s+FROM\s+'([^']+)'", stmt, re.IGNORECASE)
    if m:
        table_name = m.group(1)
        file_path = m.group(2)
        try:
            ctx.register_parquet(table_name, file_path)
        except Exception as e:
            print(f"Error registering table {table_name} from {file_path}: {e}")
    else:
        print(f"Could not parse load statement: {stmt}")

with open('../data/tpcds/queries/query_0.sql', 'r') as queries_file:
    queries_text = queries_file.read()

queries = [q.strip() for q in queries_text.strip().split("\n\n\n") if q.strip()]

def execute_query(ctx, query: str):
    statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
    result = None
    for stmt in statements:
        result = ctx.sql(stmt).collect()
    return result

def measure_query(query: str):
    def exec_fn():
        return execute_query(ctx, query)
    return measure_query_execution(exec_fn)

special_queries = {14, 23, 24, 39}

results = aggregate_benchmarks(
    queries,
    runs=3,
    measure_fn=measure_query,
    error_check=True,
    special_queries=special_queries,
    special_handler=default_special_handler
)

csv_output_path = "../results/tpcds_datafusion.csv"
fieldnames = ["Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]
write_csv_results(csv_output_path, fieldnames, results)
