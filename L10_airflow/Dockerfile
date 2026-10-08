# L10 - Airflow 3 "slim" image + the few libraries our DAGs need.
# slim = Airflow core + a minimal set of providers (smaller download than apache/airflow:3.3.2).
FROM apache/airflow:slim-3.3.2-python3.12

# curl is used by the download task; it is already in the base image.
# Install Python deps as the airflow user, pinned against Airflow's constraints where possible.
COPY requirements.txt /requirements.txt
# The [postgres] extra brings psycopg2 + asyncpg (Airflow 3 also opens an *async* connection to the metadata DB)
RUN pip install --no-cache-dir "apache-airflow[postgres]==${AIRFLOW_VERSION}" -r /requirements.txt
