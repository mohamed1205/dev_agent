"""Daily customer orders: GCS -> BigQuery -> S3

Loads each day's customer order files into BigQuery, keeps one clean row per order, exports the
day's orders (non-sensitive columns only) as Parquet, and copies the export to S3 for the AWS side.

Spec: pipelines/crm-orders-export-daily/pipeline.yaml.
Change it with a pipeline request or by asking @claude on its PR, so the spec stays in step.
"""
from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT = "atlantean-land-415108"
LOCATION = "US"
BUCKET = "atlantean-land-415108-dev-data"
SQL_DIR = "sql/crm_orders_export_daily"
S3_BUCKET = "pa-demo-exports-897729105732"  # AWS us-east-1


def source_prefix(ds_nodash: str) -> str:
    """The day's source folder in the bucket, from the run's date (YYYYMMDD)."""
    if len(ds_nodash) != 8 or not ds_nodash.isdigit():
        raise ValueError("ds_nodash must be a date as YYYYMMDD")
    return f"demo/customers/{ds_nodash}/"


default_args = {
    "owner": "mohamed1205",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="crm_orders_export_daily",
    description="Daily customer orders: GCS -> BigQuery -> S3",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 29, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    user_defined_macros={"source_prefix": source_prefix},
    tags=["pipeline:crm-orders-export-daily", "owner:mohamed1205"],
) as dag:
    wait_for_files = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_files",
        bucket=BUCKET,
        prefix="{{ source_prefix(ds_nodash) }}",
        mode="reschedule",
        poke_interval=10 * 60,
        timeout=6 * 60 * 60,
    )
    load_files = GCSToBigQueryOperator(
        task_id="load_files",
        bucket=BUCKET,
        source_objects=["{{ source_prefix(ds_nodash) }}*.csv"],
        destination_project_dataset_table="atlantean-land-415108.raw_crm.customer_orders${{ ds_nodash }}",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "customer_id", "type": "STRING"},
            {"name": "full_name", "type": "STRING"},
            {"name": "email", "type": "STRING"},
            {"name": "phone", "type": "STRING"},
            {"name": "national_id", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "amount", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_NEVER",
        time_partitioning={"type": "DAY"},
    )
    merge_customer_orders = BigQueryInsertJobOperator(
        task_id="merge_customer_orders",
        configuration={"query": {"query": "{% include '" + SQL_DIR + "/merge_customer_orders.sql' %}", "useLegacySql": False}},
        location=LOCATION,
        project_id=PROJECT,
    )
    check_rows = BigQueryCheckOperator(
        task_id="check_rows",
        sql="SELECT COUNT(*) > 0 FROM `atlantean-land-415108.crm.customer_orders` WHERE _load_id = '{{ run_id }}'",
        use_legacy_sql=False,
        location=LOCATION,
    )
    export_orders = BigQueryInsertJobOperator(
        task_id="export_orders",
        configuration={"query": {"query": "{% include '" + SQL_DIR + "/export_orders.sql' %}", "useLegacySql": False}},
        location=LOCATION,
        project_id=PROJECT,
    )

    copy_to_s3 = GCSToS3Operator(
        task_id="copy_to_s3",
        gcs_bucket=BUCKET,
        prefix="exports/customer_orders/{{ ds_nodash }}/",
        dest_s3_key=f"s3://{S3_BUCKET}/",  # keeps the GCS path: exports/customer_orders/<date>/part-*.parquet
        replace=True,
    )

    wait_for_files >> load_files >> merge_customer_orders >> check_rows >> export_orders >> copy_to_s3
