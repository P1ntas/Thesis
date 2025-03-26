FROM ubuntu:20.04

WORKDIR /workspace

RUN apt-get update && apt-get upgrade -y && apt-get install -y git python3 python3-pip sudo && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install pyarrow pandas matplotlib seaborn prettytable psutil datafusion duckdb

# RUN cd shared/scripts/tpch/ && sh benchmark.sh


CMD ["tail", "-f", "/dev/null"]