# common.py
import time
import psutil
import threading
import gc

def measure_query(query: str, exec_func):
    """
    Executes the provided query using the exec_func callback while sampling memory usage
    and I/O counters. Returns a dictionary containing latency, peak and average memory usage (in MB),
    and IOPS.

    Parameters:
      - query: The SQL query string.
      - exec_func: A callback that accepts a query string and executes it (e.g., returns results).

    Returns:
      A dict with keys: "Latency (s)", "Peak Memory Usage (MB)", "Average Memory Usage (MB)", "IOPS (ops/s)"
      If an error occurs during execution, an "error" key is also included.
    """
    process = psutil.Process(os.getpid())
    memory_samples = []
    stop_event = threading.Event()

    def sample_memory():
        while not stop_event.is_set():
            try:
                mem = process.memory_info().rss
                memory_samples.append(mem)
            except Exception as e:
                print("Error sampling memory:", e)
            time.sleep(0.1)

    mem_thread = threading.Thread(target=sample_memory)
    mem_thread.start()

    try:
        io_before = process.io_counters()
    except Exception as e:
        print("Error fetching IO counters before query:", e)
        io_before = None

    start_time = time.perf_counter()

    try:
        # Execute the query using the provided callback
        exec_func(query)
    except Exception as e:
        print("Error executing query:", e)
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
        print("Error fetching IO counters after query:", e)
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
