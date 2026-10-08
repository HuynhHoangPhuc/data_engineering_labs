"""Capstone - SILVER: bronze CDC events -> current state per source table with MERGE INTO.

For every table: take the LATEST event per primary key (highest LSN, then Kafka offset),
then MERGE into lake.silver.<table>:  op d -> DELETE,  otherwise UPDATE/INSERT.
Idempotent: running it twice gives the same result.

TODO (students):
  * make it incremental - only read bronze rows ingested since the last run
    (e.g. keep a watermark in a control table, or use Iceberg incremental reads between snapshot ids)
  * decide what to do with events that fail parsing (quarantine table?)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import TABLES, column_names, get_spark, silver_select_list  # noqa: E402


def merge_table(spark, table: str):
    cfg = TABLES[table]
    cols = column_names(table)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    latest = f"""
        SELECT op, {silver_select_list(table)}
        FROM (
            SELECT op, from_json(coalesce(after_json, before_json), '{cfg['schema']}') AS r,
                   row_number() OVER (PARTITION BY pk ORDER BY lsn DESC, kafka_offset DESC) AS rn
            FROM lake.bronze.cdc_events
            WHERE source_table = '{table}'
        ) WHERE rn = 1
    """
    # create the silver table with the right schema on first run (empty)
    spark.sql(f"CREATE TABLE IF NOT EXISTS lake.silver.{table} USING iceberg AS "
              f"SELECT {', '.join(cols)} FROM ({latest}) WHERE 1 = 0")
    on = " AND ".join(f"t.{k} = s.{k}" for k in cfg["pk"])
    set_clause = ", ".join(f"{c} = s.{c}" for c in cols)
    spark.sql(f"""
        MERGE INTO lake.silver.{table} t
        USING ({latest}) s
        ON {on}
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED AND s.updated_at IS DISTINCT FROM t.updated_at THEN UPDATE SET {set_clause}   -- skip no-op updates
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})
    """)
    n = spark.sql(f"SELECT count(*) FROM lake.silver.{table}").collect()[0][0]
    print(f"silver.{table}: {n} rows")
    return n


def run(spark):
    return {t: merge_table(spark, t) for t in TABLES}


if __name__ == "__main__":
    run(get_spark("capstone-silver"))
