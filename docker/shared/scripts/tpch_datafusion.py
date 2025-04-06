from datafusion import SessionContext
import os
import re
from common import measure_query_execution, read_tpch_queries
from common import aggregate_benchmarks, write_csv_results

os.makedirs("../results", exist_ok=True)

ctx = SessionContext()

with open('../data/tpch/parquet/load.sql', 'r') as load_file:
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
fieldnames = ["Query", "Latency (s)", "CPU Usage (%)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]
write_csv_results(csv_output_path, fieldnames, results)
