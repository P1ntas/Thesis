import duckdb
import time
import psutil
import os
import threading
import csv
import gc


os.makedirs("../results", exist_ok=True)

con = duckdb.connect(':memory:')

with open('../data/tpcds/parquet/schema.sql', 'r') as schema_file:
    schema_sql = schema_file.read()
con.execute(schema_sql)

with open('../data/tpcds/parquet/load.sql', 'r') as load_file:
    load_sql = load_file.read()
con.execute(load_sql)

with open('../data/tpcds/queries/query_0.sql', 'r') as queries_file:
    queries_text = queries_file.read()

queries = [q.strip() for q in queries_text.strip().split("\n\n\n") if q.strip()]

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
        con.execute(query).fetchall()
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

for idx, query in enumerate(queries, start=1):
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

csv_output_path = "../results/tpcds_duckdb.csv"
fieldnames = ["Query", "Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"]

with open(csv_output_path, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
        writer.writerow(row)