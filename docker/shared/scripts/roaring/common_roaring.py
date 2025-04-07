import os
import sys
import csv
from datafusion import SessionContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common import measure_query_execution


def bitmap_memory_size(*bitmap_dicts):
    total_bytes = 0
    for bitmap_dict in bitmap_dicts:
        for bitmap in bitmap_dict.values():
            total_bytes += len(bitmap.serialize()) 
    return total_bytes / (1024.0 * 1024.0)


def measure_query_duckdb(query_number: int, con, query, num_runs=3):

    runs_data = []

    def exec_fn():
        return con.execute(query).fetchall()
    
    for _ in range(num_runs):
        run_metrics = measure_query_execution(exec_fn)
        runs_data.append(run_metrics)

    averaged_result = _aggregate_runs(runs_data)
    averaged_result["Query"] = query_number
    return averaged_result


def measure_query_datafusion(query_number: int, ctx: SessionContext, query_str: str, num_runs=3):

    runs_data = []

    def exec_fn():
        df = ctx.sql(query_str)
        return df.collect()
    
    for _ in range(num_runs):
        run_metrics = measure_query_execution(exec_fn)
        runs_data.append(run_metrics)

    averaged_result = _aggregate_runs(runs_data)
    averaged_result["Query"] = query_number
    return averaged_result

def write_csv_results(csv_path, fieldnames, rows):
    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerows(rows)

def _aggregate_runs(runs_data):
    if not runs_data:
        return {}
    
    aggregated = {}
    num_runs = len(runs_data)
    
    for key in runs_data[0].keys():
        values = [rd[key] for rd in runs_data if key in rd]
        
        try:
            total = sum(values)
            aggregated[key] = total / num_runs
        except TypeError:
            aggregated[key] = values[0] if values else None
    
    return aggregated
