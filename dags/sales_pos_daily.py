"""Daily branch sales for Finance

Loads the nightly POS files of the Riyadh, Jeddah and Dammam branches, keeps one clean row per transaction
(without customer phone numbers), builds daily revenue per branch for the Finance dashboard, and copies the
day's figures to S3 for Finance HQ.

Built by the pipeline builder for https://github.com/mohamed1205/dev_agent/issues/32. Spec: pipelines/sales-pos-daily/pipeline.yaml.
Change it with a pipeline request or by asking @claude on its PR, so the spec stays in step.
"""
from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
    GCSUploadSessionCompleteSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Write every table and bucket out in full; the checks compare them with the allow-lists in .platform-agents/tools.yaml.
PROJECT = "atlantean-land-415108"
LOCATION = "US"
BUCKET = "atlantean-land-415108-dev-data"
S3_DESTINATION = "s3://pa-demo-exports-897729105732/"
AWS_CONN_ID = "aws_default"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="sales_pos_daily",
    description="Daily branch sales for Finance",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 11, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Finance needs sales.daily_branch_sales by 04:00 UTC: a run still going at 04:00 fails loudly.
    dagrun_timeout=pendulum.duration(minutes=90),
    default_args=default_args,
    tags=["pipeline:sales-pos-daily", "owner:data-platform"],
) as dag:
    # The run on day D+1 at 02:30 UTC loads folder pos/<D>/ ({{ ds }}): at least one file per branch.
    wait_for_ruh = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_ruh",
        bucket=BUCKET,
        prefix="pos/{{ ds }}/pos_ruh_",
        deferrable=True,
        poke_interval=60,
        timeout=45 * 60,
    )
    wait_for_jed = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_jed",
        bucket=BUCKET,
        prefix="pos/{{ ds }}/pos_jed_",
        deferrable=True,
        poke_interval=60,
        timeout=45 * 60,
    )
    wait_for_dmm = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_dmm",
        bucket=BUCKET,
        prefix="pos/{{ ds }}/pos_dmm_",
        deferrable=True,
        poke_interval=60,
        timeout=45 * 60,
    )
    # Then 5 minutes with no new file in the folder.
    wait_for_quiet = GCSUploadSessionCompleteSensor(
        task_id="wait_for_quiet",
        bucket=BUCKET,
        prefix="pos/{{ ds }}/",
        inactivity_period=5 * 60,
        min_objects=3,
        deferrable=True,
        poke_interval=60,
        timeout=20 * 60,
    )
    # Raw layer: the files as they arrive, every column STRING. WRITE_TRUNCATE replaces only this run's partition.
    load_files = GCSToBigQueryOperator(
        task_id="load_files",
        bucket=BUCKET,
        source_objects=["pos/{{ ds }}/pos_*.csv"],
        destination_project_dataset_table="atlantean-land-415108.raw_pos.pos_transactions${{ ds_nodash }}",
        source_format="CSV",
        skip_leading_rows=1,
        field_delimiter=",",
        schema_fields=[
            {"name": "transaction_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "branch_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "transaction_date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "transaction_time", "type": "STRING", "mode": "NULLABLE"},
            {"name": "transaction_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_sku", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity", "type": "STRING", "mode": "NULLABLE"},
            {"name": "unit_price", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "STRING", "mode": "NULLABLE"},
            {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payment_method", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cashier_id", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        # The table is created by its Flyway definition (bigquery_tables/raw_pos/pos_transactions.sql).
        create_disposition="CREATE_NEVER",
        time_partitioning={"type": "DAY"},
        location=LOCATION,
        project_id=PROJECT,
    )
    # Fails the run before anything reaches the sales dataset: branch codes, date and time formats,
    # currency, amounts, refunds, and re-sent rows that differ.
    validate_raw = BigQueryCheckOperator(
        task_id="validate_raw",
        sql="{% include 'sql/sales_pos_daily/validate_raw.sql' %}",
        use_legacy_sql=False,
        location=LOCATION,
    )
    merge_transactions = BigQueryInsertJobOperator(
        task_id="merge_transactions",
        configuration={"query": {"query": "{% include 'sql/sales_pos_daily/merge_transactions.sql' %}", "useLegacySql": False}},
        location=LOCATION,
        project_id=PROJECT,
    )
    build_daily_sales = BigQueryInsertJobOperator(
        task_id="build_daily_sales",
        configuration={"query": {"query": "{% include 'sql/sales_pos_daily/build_daily_sales.sql' %}", "useLegacySql": False}},
        location=LOCATION,
        project_id=PROJECT,
    )
    check_transactions = BigQueryCheckOperator(
        task_id="check_transactions",
        sql="{% include 'sql/sales_pos_daily/check_transactions.sql' %}",
        use_legacy_sql=False,
        location=LOCATION,
    )
    check_daily_sales = BigQueryCheckOperator(
        task_id="check_daily_sales",
        sql="{% include 'sql/sales_pos_daily/check_daily_sales.sql' %}",
        use_legacy_sql=False,
        location=LOCATION,
    )
    stage_export = BigQueryInsertJobOperator(
        task_id="stage_export",
        configuration={"query": {"query": "{% include 'sql/sales_pos_daily/stage_export.sql' %}", "useLegacySql": False}},
        location=LOCATION,
        project_id=PROJECT,
    )
    # Copies gs://<bucket>/finance/daily_branch_sales/<D>/ to the same key in S3, overwriting the day's file
    # on a rerun (the AWS role can put objects, not delete them). Keyless: aws_default uses web identity federation.
    copy_export_to_s3 = GCSToS3Operator(
        task_id="copy_export_to_s3",
        gcs_bucket=BUCKET,
        prefix="finance/daily_branch_sales/{{ ds }}/",
        dest_aws_conn_id=AWS_CONN_ID,
        dest_s3_key=S3_DESTINATION,
        replace=True,
        keep_directory_structure=True,
    )

    [wait_for_ruh, wait_for_jed, wait_for_dmm] >> wait_for_quiet >> load_files >> validate_raw
    validate_raw >> merge_transactions >> build_daily_sales >> check_transactions >> check_daily_sales
    check_daily_sales >> stage_export >> copy_export_to_s3
