import cppyy
import random
import time

cppyy.add_include_path(".")          
cppyy.load_library("./lib/libfast.so")    
cppyy.include("./src/fast.hpp")   

def test_integer_search():
    print("\n=== Testing Integer Search ===")
    
    keys = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    
    FastIndex = cppyy.gbl.fast.FastIndex
    index = FastIndex(keys)
    
    print(f"Created FAST index with {len(keys)} elements, padded size = {index.size()}")
    
    print("\nSearching for existing keys:")
    for key in [30, 70, 100]:
        start = time.perf_counter_ns()
        result = index.search(key)
        elapsed = time.perf_counter_ns() - start
        print(f"Search for {key}: result = {result}, took {elapsed} ns")
    
    print("\nSearching for non-existing keys:")
    for key in [15, 55, 120]:
        start = time.perf_counter_ns()
        result = index.search(key)
        elapsed = time.perf_counter_ns() - start
        print(f"Search for {key}: result = {result}, took {elapsed} ns")
    
    print("\nBenchmarking with 10,000 random queries:")
    queries = random.choices(keys, k=10000)
    start = time.perf_counter_ns()
    s = 0
    for k in queries:
        s += index.search(k)
    elapsed = time.perf_counter_ns() - start
    print(f"Average latency = {elapsed / 10000:.1f} ns, checksum = {s}")

def test_date_search():
    print("\n=== Testing Date Search ===")
    
    date_keys = [
        "1998-01-15", "1998-03-20", "1998-05-10", 
        "1998-07-30", "1998-09-05", "1998-11-25", 
        "1998-12-01", "1999-01-10", "1999-02-15", "1999-03-22"
    ]
    
    FastIndex = cppyy.gbl.fast.FastIndex
    index = FastIndex(date_keys)
    
    print(f"Created FAST index with {len(date_keys)} date elements, padded size = {index.size()}")
    
    print("\nSearching for existing dates:")
    for date_key in ["1998-05-10", "1998-12-01", "1999-03-22"]:
        start = time.perf_counter_ns()
        result = index.searchDate(date_key)
        elapsed = time.perf_counter_ns() - start
        print(f"Search for {date_key}: result = {result}, took {elapsed} ns")
    
    print("\nSearching for dates not in the index:")
    for date_key in ["1998-06-15", "1999-01-01", "1997-12-31"]:
        start = time.perf_counter_ns()
        result = index.searchDate(date_key)
        elapsed = time.perf_counter_ns() - start
        print(f"Search for {date_key}: result = {result}, took {elapsed} ns")
    
    print("\nBenchmarking with 10,000 random date queries:")
    queries = random.choices(date_keys, k=10000)
    start = time.perf_counter_ns()
    s = 0
    for date in queries:
        s += index.searchDate(date)
    elapsed = time.perf_counter_ns() - start
    print(f"Average latency = {elapsed / 10000:.1f} ns, checksum = {s}")

if __name__ == "__main__":
    test_integer_search()
    test_date_search()
    
    print("\nAll tests completed!")