from __future__ import annotations
import re, time, pathlib, numpy as np, pyarrow.parquet as pq, pyarrow as pa, pyarrow.compute as pc
import cppyy

cppyy.add_include_path(".")
cppyy.load_library("./fast/lib/libfast.so")
cppyy.include("./fast/src/fast.hpp")
FastIndex = cppyy.gbl.fast.FastIndex
StdVecI32 = cppyy.gbl.std.vector['int32_t']

SQL_RANGE = re.compile(r"l_shipdate\s*<=\s*date\s*'([^']+)'", re.I)


def extract_limit(sql_file: pathlib.Path) -> np.int32:
    m = SQL_RANGE.search(sql_file.read_text())
    if not m:
        raise RuntimeError("no `l_shipdate <= date 'YYYY-MM-DD'` predicate")
    return np.datetime64(m.group(1), "D").astype(np.int32)


def build_vector_from_parquet(col_path: pathlib.Path) -> StdVecI32:
    pf   = pq.ParquetFile(col_path)
    vec  = StdVecI32()
    vec.reserve(pf.metadata.num_rows)    

    for rg in range(pf.num_row_groups):
        carray = pf.read_row_group(rg, columns=["l_shipdate"]).column(0)
        for chunk in carray.chunks:
            np_view = (chunk
                       .to_numpy(zero_copy_only=False)
                       .astype("datetime64[D]").astype(np.int32))
            vec += np_view.tolist()  
    return vec



def write_filtered_rows(src_path: pathlib.Path,
                        out_path: pathlib.Path,
                        limit_days: int):
    pf      = pq.ParquetFile(src_path)
    writer  = None
    limit_scalar = pa.scalar(limit_days, pa.int32())   

    for rg in range(pf.num_row_groups):
        tbl   = pf.read_row_group(rg)
        sd32  = pc.cast(tbl["l_shipdate"], pa.int32())
        mask  = pc.less_equal(sd32, limit_scalar)
        if pc.any(mask).as_py():                      
            filtered = tbl.filter(mask)
            if writer is None:
                writer = pq.ParquetWriter(out_path,
                                          filtered.schema,
                                          compression="snappy")
            writer.write_table(filtered)

    if writer is not None:
        writer.close()


if __name__ == "__main__":
    sql_path = pathlib.Path("../data/tpch/queries/1.sql")
    pq_path  = pathlib.Path("../data/tpch/parquet/lineitem.parquet")
    out_path = pq_path.with_name("lineitem_filtered.parquet")

    limit = extract_limit(sql_path)
    print("Predicate  :", np.datetime_as_string(limit.astype("datetime64[D]")),
          f"(days={limit})")

    vec_cpp = build_vector_from_parquet(pq_path)
    print("Rows       :", f"{vec_cpp.size():,}")

    fast = FastIndex(vec_cpp, 3)
    t0 = time.perf_counter_ns()
    rank = fast.upper_bound(int(limit))
    print(f"upper_bound() = {rank:,}   latency = {time.perf_counter_ns() - t0} ns")
    print("FAST size  :", f"{fast.size():,}")

    print("Writing filtered Parquet …")
    write_filtered_rows(pq_path, out_path, int(limit))
    print("Done  →", out_path)
