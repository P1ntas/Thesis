COPY customer FROM '../data/tpch/parquet/customer.parquet' (FORMAT 'parquet');
COPY lineitem FROM '../data/tpch/parquet/lineitem.parquet' (FORMAT 'parquet');
COPY nation FROM '../data/tpch/parquet/nation.parquet' (FORMAT 'parquet');
COPY orders FROM '../data/tpch/parquet/orders.parquet' (FORMAT 'parquet');
COPY part FROM '../data/tpch/parquet/part.parquet' (FORMAT 'parquet');
COPY partsupp FROM '../data/tpch/parquet/partsupp.parquet' (FORMAT 'parquet');
COPY region FROM '../data/tpch/parquet/region.parquet' (FORMAT 'parquet');
COPY supplier FROM '../data/tpch/parquet/supplier.parquet' (FORMAT 'parquet');
