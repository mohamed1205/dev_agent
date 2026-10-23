from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    "orders_summary",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["agent-test"],
):
    BigQueryInsertJobOperator(
        task_id="build_orders_summary",
        location="US",
        retries=0,
        configuration={
            "query": {
                "query": (
                    "CREATE OR REPLACE TABLE `atlantean-land-415108.dev_raw.orders_summary` AS "
                    "SELECT order_id, SUM(amout) AS total_amount "
                    "FROM `atlantean-land-415108.dev_raw.orders` "
                    "GROUP BY order_id"
                ),
                "useLegacySql": False,
            }
        },
    )
