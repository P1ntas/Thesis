from datafusion import SessionContext
import time
import psutil
import os
import threading
import csv
import gc
import re

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
            print(f"Registering table {table_name} from {file_path}")
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
    process = psutil.Process(os.getpid())
    memory_samples = []
    stop_event = threading.Event()

    def sample_memory():
        while not stop_event.is_set():
            try:
                mem = process.memory_info().rss
                memory_samples.append(mem)
            except Exception as e:
                print(f"Error sampling memory: {e}")
            time.sleep(0.1)

    mem_thread = threading.Thread(target=sample_memory)
    mem_thread.start()

    try:
        io_before = process.io_counters()
    except Exception as e:
        print(f"Error fetching IO counters before query: {e}")
        io_before = None

    start_time = time.perf_counter()

    try:
        execute_query(ctx, query)
    except Exception as e:
        print(f"Error executing query: {e}")
        stop_event.set()
        mem_thread.join()
        return {
            "Latency (s)": None,
            "Peak Memory Usage (MB)": None,
            "Average Memory Usage (MB)": None,
            "IOPS (ops/s)": None,
            "error": str(e)
        }

    end_time = time.perf_counter()

    try:
        io_after = process.io_counters()
    except Exception as e:
        print(f"Error fetching IO counters after query: {e}")
        io_after = None

    stop_event.set()
    mem_thread.join()

    latency = end_time - start_time
    if memory_samples:
        peak_memory = max(memory_samples)
        avg_memory = sum(memory_samples) / len(memory_samples)
    else:
        peak_memory = avg_memory = 0

    if io_before is not None and io_after is not None:
        read_count = io_after.read_count - io_before.read_count
        write_count = io_after.write_count - io_before.write_count
        total_iops = (read_count + write_count) / latency if latency > 0 else 0
    else:
        total_iops = None

    peak_memory_mb = peak_memory / (1024 * 1024)
    avg_memory_mb = avg_memory / (1024 * 1024)

    gc.collect()

    return {
        "Latency (s)": latency,
        "Peak Memory Usage (MB)": peak_memory_mb,
        "Average Memory Usage (MB)": avg_memory_mb,
        "IOPS (ops/s)": total_iops
    }

results = []
runs = 3 

special_queries = {14, 23, 24, 39}

for idx, query in enumerate(queries, start=1):
    if idx in special_queries:
        subqueries = re.split(r";\s*\nwith", query, flags=re.IGNORECASE)
        for i in range(1, len(subqueries)):
            subqueries[i] = "with " + subqueries[i].strip()
        subqueries = [sq.strip().rstrip(";") for sq in subqueries if sq.strip()]
        
        total_latency = 0.0
        total_peak_memory = 0.0
        total_avg_memory = 0.0
        total_iops = 0.0
        subquery_count = 0
        
        for subquery in subqueries:
            metrics = measure_query(subquery)
            if metrics.get("error"):
                print(f"Error executing a subquery in query {idx}: {metrics['error']}")
                continue
            total_latency += metrics["Latency (s)"]
            total_peak_memory += metrics["Peak Memory Usage (MB)"]
            total_avg_memory += metrics["Average Memory Usage (MB)"]
            total_iops += metrics["IOPS (ops/s)"]
            subquery_count += 1
            time.sleep(0.5) 
        
        if subquery_count == 0:
            avg_metrics = {
                "Query": idx,
                "Latency (s)": None,
                "Peak Memory Usage (MB)": None,
                "Average Memory Usage (MB)": None,
                "IOPS (ops/s)": None
            }
        else:
            avg_metrics = {
                "Query": idx,
                "Latency (s)": total_latency,
                "Peak Memory Usage (MB)": total_peak_memory / subquery_count,
                "Average Memory Usage (MB)": total_avg_memory / subquery_count,
                "IOPS (ops/s)": total_iops / subquery_count
            }
        results.append(avg_metrics)
        print(f"Query {idx} executed (special handling for multi-statement)")
    else:
        sum_latency = 0.0
        sum_peak_memory = 0.0
        sum_avg_memory = 0.0
        sum_iops = 0.0
        valid_runs = 0

        for _ in range(runs):
            metrics = measure_query(query)
            if metrics.get("error"):
                print(f"Error on query {idx} run: {metrics['error']}")
                continue
            sum_latency += metrics["Latency (s)"]
            sum_peak_memory += metrics["Peak Memory Usage (MB)"]
            sum_avg_memory += metrics["Average Memory Usage (MB)"]
            sum_iops += metrics["IOPS (ops/s)"]
            valid_runs += 1
            time.sleep(0.5) 

        if valid_runs == 0:
            avg_metrics = {
                "Query": idx,
                "Latency (s)": None,
                "Peak Memory Usage (MB)": None,
                "Average Memory Usage (MB)": None,
                "IOPS (ops/s)": None
            }
        else:
            avg_metrics = {
                "Query": idx,
                "Latency (s)": sum_latency / valid_runs,
                "Peak Memory Usage (MB)": sum_peak_memory / valid_runs,
                "Average Memory Usage (MB)": sum_avg_memory / valid_runs,
                "IOPS (ops/s)": sum_iops / valid_runs
            }
        results.append(avg_metrics)
    time.sleep(1)

csv_output_path = "../results/tpcds_datafusion.csv"
fieldnames = ["Query", "Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]

with open(csv_output_path, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
        writer.writerow(row)
