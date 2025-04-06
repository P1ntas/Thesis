import os
import time
import gc
import threading
import psutil
import csv
import re

def measure_query_execution(execution_fn):
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

    cpu_times_before = process.cpu_times()
    start_time = time.perf_counter()

    try:
        result = execution_fn()
    except Exception as e:
        print(f"Error executing query: {e}")
        stop_event.set()
        mem_thread.join()
        return {
            "Latency (s)": None,
            "Peak Memory Usage (MB)": None,
            "Average Memory Usage (MB)": None,
            "IOPS (ops/s)": None,
            "CPU Usage (%)": None,
            "error": str(e)
        }

    end_time = time.perf_counter()
    cpu_times_after = process.cpu_times()

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
        peak_memory = 0
        avg_memory = 0
    peak_memory_mb = peak_memory / (1024 * 1024)
    avg_memory_mb = avg_memory / (1024 * 1024)

    if io_before and io_after and latency > 0:
        read_count = io_after.read_count - io_before.read_count
        write_count = io_after.write_count - io_before.write_count
        total_iops = (read_count + write_count) / latency
    else:
        total_iops = None

    used_cpu_time = (
        (cpu_times_after.user + cpu_times_after.system)
        - (cpu_times_before.user + cpu_times_before.system)
    )
    cpu_usage_percent = (used_cpu_time / latency) * 100 if latency > 0 else None

    gc.collect()

    return {
        "Latency (s)": latency,
        "Peak Memory Usage (MB)": peak_memory_mb,
        "Average Memory Usage (MB)": avg_memory_mb,
        "IOPS (ops/s)": total_iops,
        "CPU Usage (%)": cpu_usage_percent,
        "result": result
    }

def read_tpch_queries(query_number: int):
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
        return (query_str1, query_str2, query_str3)
    else:
        file_path = f"../data/tpch/queries/{query_number}.sql"
        with open(file_path, "r") as file:
            query_str = file.read()
        return query_str

def aggregate_runs_for_query(query, runs, measure_fn,
                             error_check=False,
                             sleep_between_run=0.5,
                             latency_mode="average"):
    sum_latency = 0.0
    sum_peak_memory = 0.0
    sum_avg_memory = 0.0
    sum_iops = 0.0
    sum_cpu_usage = 0.0
    valid_runs = 0

    for _ in range(runs):
        metrics = measure_fn(query)
        if error_check and metrics.get("error"):
            print(f"Error on query run: {metrics['error']}")
            continue
        sum_latency += metrics["Latency (s)"]
        sum_peak_memory += metrics["Peak Memory Usage (MB)"]
        sum_avg_memory += metrics["Average Memory Usage (MB)"]
        if metrics["IOPS (ops/s)"] is not None:
            sum_iops += metrics["IOPS (ops/s)"]
        if metrics["CPU Usage (%)"] is not None:
            sum_cpu_usage += metrics["CPU Usage (%)"]
        valid_runs += 1
        time.sleep(sleep_between_run)

    if valid_runs == 0:
        return {
            "Latency (s)": None,
            "Peak Memory Usage (MB)": None,
            "Average Memory Usage (MB)": None,
            "IOPS (ops/s)": None,
            "CPU Usage (%)": None
        }

    if latency_mode == "average":
        return {
            "Latency (s)": sum_latency / valid_runs,
            "Peak Memory Usage (MB)": sum_peak_memory / valid_runs,
            "Average Memory Usage (MB)": sum_avg_memory / valid_runs,
            "IOPS (ops/s)": sum_iops / valid_runs,
            "CPU Usage (%)": sum_cpu_usage / valid_runs
        }
    elif latency_mode == "sum":
        return {
            "Latency (s)": sum_latency,
            "Peak Memory Usage (MB)": sum_peak_memory / valid_runs,
            "Average Memory Usage (MB)": sum_avg_memory / valid_runs,
            "IOPS (ops/s)": sum_iops / valid_runs,
            "CPU Usage (%)": sum_cpu_usage / valid_runs
        }

def aggregate_benchmarks(queries,
                         runs,
                         measure_fn,
                         get_query_label=lambda idx, query: idx,
                         error_check=False,
                         sleep_between_run=0.5,
                         sleep_between_queries=1.0,
                         special_queries=None,
                         special_handler=None):
    results = []
    for idx, query in enumerate(queries, start=1):
        label = get_query_label(idx, query)
        if special_queries and idx in special_queries and special_handler:
            metrics = special_handler(idx, query, measure_fn, runs)
        else:
            metrics = aggregate_runs_for_query(query,
                                               runs,
                                               measure_fn,
                                               error_check,
                                               sleep_between_run,
                                               latency_mode="average")
        metrics["Query"] = label
        results.append(metrics)
        time.sleep(sleep_between_queries)
    return results

def write_csv_results(csv_output_path, fieldnames, results):
    with open(csv_output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

def default_special_handler(idx, query, measure_fn, runs):
    subqueries = re.split(r";\\s*\\nwith", query, flags=re.IGNORECASE)
    for i in range(1, len(subqueries)):
        subqueries[i] = "with " + subqueries[i].strip()
    subqueries = [sq.strip().rstrip(";") for sq in subqueries if sq.strip()]

    total_latency = 0.0
    total_peak_memory = 0.0
    total_avg_memory = 0.0
    total_iops = 0.0
    total_cpu_usage = 0.0
    subquery_count = 0

    for subquery in subqueries:
        metrics = measure_fn(subquery)
        if metrics.get("error"):
            print(f"Error executing a subquery in query {idx}: {metrics['error']}")
            continue
        total_latency += metrics["Latency (s)"]
        total_peak_memory += metrics["Peak Memory Usage (MB)"]
        total_avg_memory += metrics["Average Memory Usage (MB)"]
        if metrics["IOPS (ops/s)"] is not None:
            total_iops += metrics["IOPS (ops/s)"]
        if metrics["CPU Usage (%)"] is not None:
            total_cpu_usage += metrics["CPU Usage (%)"]
        subquery_count += 1
        time.sleep(0.5)

    if subquery_count == 0:
        return {
            "Latency (s)": None,
            "Peak Memory Usage (MB)": None,
            "Average Memory Usage (MB)": None,
            "IOPS (ops/s)": None,
            "CPU Usage (%)": None
        }
    return {
        "Latency (s)": total_latency,
        "Peak Memory Usage (MB)": total_peak_memory / subquery_count,
        "Average Memory Usage (MB)": total_avg_memory / subquery_count,
        "IOPS (ops/s)": total_iops / subquery_count,
        "CPU Usage (%)": total_cpu_usage / subquery_count
    }
