FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    'protobuf>=3.19,<5.0' \
    dbt-core==1.9.0 \
    dbt-postgres==1.9.0