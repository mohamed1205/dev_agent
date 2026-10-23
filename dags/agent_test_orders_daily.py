"""Daily order CSV files into BigQuery

Loads each day's order CSV files from Cloud Storage into the raw table dev_raw.agent_test_orders,
replacing that day's partition so reruns are safe.

Built by the pipeline builder for https://github.com/mohamed1205/dev_agent/issues/6. Spec: pipelines/agent-test-orders-daily/pipeline.yaml.
Change it with a pipeline request or by asking @claude on its PR, so the spec stays in step.
"""
from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Write every table and bucket out in full; the checks compare them with the allow-lists in .platform-agents/tools.yaml.
PROJECT = "atlantean-land-415108"
LOCATION = "US"
BUCKET = "atlantean-land-415108-dev-data"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="agent_test_orders_daily",
    description="Daily order CSV files into BigQuery",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["pipeline:agent-test-orders-daily", "owner:data-platform"],
) as dag:
    create_tables = BigQueryInsertJobOperator(
        task_id="create_tables",
        configuration={
            "query": {
                "query": "{% include 'sql/agent_test_orders_daily/create_tables.sql' %}",
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        project_id=PROJECT,
    )
    wait_for_files = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_files",
        bucket=BUCKET,
        prefix="agent-test/orders/{{ ds_nodash }}/",
        deferrable=True,
        timeout=6 * 60 * 60,
    )
    load_files = GCSToBigQueryOperator(
        task_id="load_files",
        bucket=BUCKET,
        source_objects=["agent-test/orders/{{ ds_nodash }}/*.csv"],
        destination_project_dataset_table="atlantean-land-415108.dev_raw.agent_test_orders${{ ds_nodash }}",
        source_format="CSV",
        field_delimiter=",",
        skip_leading_rows=1,
        autodetect=False,
        schema_fields=[
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
        location=LOCATION,
    )
    check_rows = BigQueryCheckOperator(
        task_id="check_rows",
        sql="SELECT COUNT(*) > 0 FROM `atlantean-land-415108.dev_raw.agent_test_orders` WHERE _PARTITIONDATE = '{{ ds }}'",
        use_legacy_sql=False,
        location=LOCATION,
    )

    create_tables >> wait_for_files >> load_files >> check_rows
