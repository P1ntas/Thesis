import glob
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK_SIZE = 100000

for dat_file in glob.glob("*.dat"):
    print(f"Processing {dat_file} in chunks...")

    original_dir = os.path.dirname(dat_file)
    out_dir = os.path.normpath(os.path.join(original_dir, "../../../docker/shared/data/tpcds/parquet/"))
    os.makedirs(out_dir, exist_ok=True)
    
    output_filename = os.path.splitext(os.path.basename(dat_file))[0] + ".parquet"
    parquet_file = os.path.join(out_dir, output_filename)
    
    writer = None

    for i, chunk in enumerate(pd.read_csv(dat_file, sep="|", header=None, chunksize=CHUNK_SIZE)):
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        
        if writer is None:
            writer = pq.ParquetWriter(parquet_file, table.schema)
        
        writer.write_table(table)
        print(f"Processed chunk {i+1}")

    if writer is not None:
        writer.close()
        print(f"Saved {parquet_file}")
