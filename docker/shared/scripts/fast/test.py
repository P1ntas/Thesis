import random, time, numpy as np
import cppyy

cppyy.add_include_path(".")          
cppyy.load_library("./lib/libfast.so")    
cppyy.include("./src/fast.hpp")   

def read_data(filepath):
    with open(filepath, "r") as f:
        return np.fromfile(filepath, dtype=np.int32).tolist()

import sys
if len(sys.argv) < 2:
    print(f"Usage: python {sys.argv[0]} DATA_PATH")
    sys.exit(1)

data_path = sys.argv[1]
keys = read_data(data_path)
print(f"num elements: {len(keys)}")

keys_clone = list(keys)

FastIndex = cppyy.gbl.fast.FastIndex
index = FastIndex(keys)
print("padded size =", index.size())

Q = 2_000_000
queries = random.choices(keys_clone, k=Q)

start = time.perf_counter_ns()
s = 0
for k in queries:
    s += index.search(k)
elapsed = time.perf_counter_ns() - start

print(f"avg latency = {elapsed / Q:.1f} ns   checksum = {s}")
