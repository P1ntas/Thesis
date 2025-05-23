import os
import sys
import csv
from typing import Dict, Iterable
from datafusion import SessionContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import measure_query_execution


def aggregate_metrics(*metric_dicts: Dict[str, float]) -> Dict[str, float]:
    out: Dict[str, float] = {}

    tot_latency      = 0.0
    cpu_weighted_sum = 0.0
    iops_weighted_sum = 0.0

    for md in metric_dicts:
        if not md:
            continue

        lat = md.get("Latency (s)")
        if lat is not None:
            out["Latency (s)"] = (out.get("Latency (s)", 0.0) + lat)
            tot_latency += lat

        for mkey in ("Peak Memory Usage (MB)", "Average Memory Usage (MB)"):
            mem = md.get(mkey)
            if mem is not None:
                out[mkey] = max(out.get(mkey, 0.0), mem)

        cpu  = md.get("CPU Usage (%)")
        if cpu is not None and lat is not None:
            cpu_weighted_sum  += cpu  * lat

        iops = md.get("IOPS (ops/s)")
        if iops is not None and lat is not None:
            iops_weighted_sum += iops * lat

    if tot_latency > 0:
        if cpu_weighted_sum:
            out["CPU Usage (%)"] = cpu_weighted_sum / tot_latency
        if iops_weighted_sum:
            out["IOPS (ops/s)"]  = iops_weighted_sum / tot_latency

    return out


def fast_tree_memory_size(*fast_tree_dicts):
    total_bytes = 0
    for ft_dict in fast_tree_dicts:
        for fast_tree in ft_dict.values():
            total_bytes += fast_tree.size()
    return total_bytes / (1024 * 1024) 

def measure_query_duckdb(query_number: int, con, query, num_runs: int = 3):
    con.execute("SET explain_output = 'all';")
    con.execute("PRAGMA enable_profiling = json;")
    con.execute("SET profiling_mode = detailed;")
    profile_path = f"../results/kdtree/analyze/{query_number}.json"
    os.makedirs(os.path.dirname(profile_path), exist_ok=True)
    con.execute(f"SET profiling_output = '{profile_path}';")
    con.execute(query)
    con.execute("PRAGMA disable_profiling;")
    runs = [measure_query_execution(lambda: con.execute(query).fetchall())
            for _ in range(num_runs)]
    result = _aggregate_runs(runs)
    result["Query"] = query_number
    return result


def measure_query_datafusion(
    query_number: int,
    ctx: SessionContext,
    query_str: str,
    num_runs: int = 3,
):
    runs = [measure_query_execution(lambda: ctx.sql(query_str).collect())
            for _ in range(num_runs)]
    result = _aggregate_runs(runs)
    result["Query"] = query_number
    return result


def write_csv_results(csv_path, fieldnames, rows):
    first_time = (not os.path.exists(csv_path)) or os.path.getsize(csv_path) == 0
    with open(csv_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if first_time:
            w.writeheader()
        w.writerows(rows)


def _aggregate_runs(runs_data):
    if not runs_data:
        return {}

    agg: Dict[str, float] = {}
    num = len(runs_data)
    numeric = (int, float)

    keys = set().union(*(rd.keys() for rd in runs_data))

    for k in keys:
        vals = [rd.get(k) for rd in runs_data if rd.get(k) is not None]

        if not vals:
            continue

        if all(isinstance(v, numeric) for v in vals):
            if k == "Peak Memory Usage (MB)":
                agg[k] = max(vals)
            else:
                agg[k] = sum(vals) / len(vals)
        else:
            agg[k] = vals[0]

    return agg