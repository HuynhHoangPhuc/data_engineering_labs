# Capstone Airflow image: Airflow 3.3 slim + the thin Spark Connect client (no JVM!)
FROM apache/airflow:slim-3.3.2-python3.12
# [postgres] extra = psycopg2 + asyncpg for the metadata DB
RUN pip install --no-cache-dir "apache-airflow[postgres]==${AIRFLOW_VERSION}" "pyspark-client==4.1.3"
