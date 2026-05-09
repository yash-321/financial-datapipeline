FROM apache/airflow:2.7.3-python3.10

RUN pip install --no-cache-dir \
    "dbt-snowflake==1.7.5" \
    "snowflake-connector-python<4.0.0" \
    "protobuf<5.0.0" \
    "cryptography<44" \
    tabulate \
    python-dotenv