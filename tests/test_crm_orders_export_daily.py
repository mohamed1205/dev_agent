"""Tests for the crm-orders-export-daily pipeline (Composer, composer-dev).

Spec: pipelines/crm-orders-export-daily/pipeline.yaml. Expectations come from the spec and
.platform-agents/conventions.yaml, not from the code. No network, no connections, no real data:
every value below is made up or copied from the spec.
"""
from __future__ import annotations

import datetime
import pathlib

import jinja2
import pytest
from airflow.models import DagBag
from hypothesis import given, strategies as st

from crm_orders_export_daily import source_prefix

DAG_ID = "crm_orders_export_daily"
PIPELINE_ID = "crm-orders-export-daily"

# pipelines/crm-orders-export-daily/pipeline.yaml: steps, in run order
SPEC_STEPS = [
    "wait_for_files",
    "load_files",
    "merge_customer_orders",
    "check_rows",
    "export_orders",
    "copy_to_s3",
]


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="dags", include_examples=False)  # composer-dev's repo_path
    assert not bag.import_errors, bag.import_errors
    return bag.dags[DAG_ID]  # not get_dag(), which also looks in Airflow's metadata database


def _render(dag, template: str, ds_nodash: str) -> str:
    """Renders a templated field with the DAG's own macros, without Airflow's metadata database."""
    return jinja2.Template(template).render(ds_nodash=ds_nodash, **dag.user_defined_macros)


# --- DAG structure -------------------------------------------------------------------------------


def test_has_exactly_the_spec_steps(dag):
    assert sorted(dag.task_ids) == sorted(SPEC_STEPS)


def test_steps_run_one_after_another_in_spec_order(dag):
    # spec: wait_for_files -> load_files -> merge_customer_orders -> check_rows -> export_orders -> copy_to_s3
    assert dag.get_task("wait_for_files").upstream_task_ids == set()
    for before, after in zip(SPEC_STEPS, SPEC_STEPS[1:]):
        assert dag.get_task(before).downstream_task_ids == {after}, (before, after)
        assert dag.get_task(after).upstream_task_ids == {before}, (before, after)
    assert dag.get_task("copy_to_s3").downstream_task_ids == set()


def test_check_rows_runs_after_the_merge_and_before_anything_leaves_bigquery(dag):
    # check_rows checks crm.customer_orders, which merge_customer_orders writes; export_orders and
    # copy_to_s3 must only run once that check passed
    check = dag.get_task("check_rows")
    assert "merge_customer_orders" in check.get_flat_relative_ids(upstream=True)
    assert {"export_orders", "copy_to_s3"} <= check.get_flat_relative_ids(upstream=False)
    assert "atlantean-land-415108.crm.customer_orders" in check.sql
    assert "{{ run_id }}" in check.sql  # it checks this run's rows, not any day's
    assert check.use_legacy_sql is False


def test_schedule_and_timezone_follow_the_spec(dag):
    # spec: schedule "0 6 * * *", timezone UTC
    assert dag.timetable.summary == "0 6 * * *"
    assert dag.timezone.name == "UTC"


def test_conventions(dag):
    # spec owner, and conventions.yaml defaults: retries 2, 5 minutes apart, one active run, no catchup
    assert dag.owner == "mohamed1205"
    assert dag.default_args["retries"] == 2
    assert dag.default_args["retry_delay"] == datetime.timedelta(minutes=5)
    for task in dag.tasks:
        assert task.retries == 2, task.task_id
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert "pipeline:crm-orders-export-daily" in dag.tags
    assert "owner:mohamed1205" in dag.tags


def test_wait_for_files_waits_for_the_days_source_folder(dag):
    # spec: wait_for_files reads gs://atlantean-land-415108-dev-data/demo/customers/{{ ds_nodash }}/
    sensor = dag.get_task("wait_for_files")
    assert sensor.bucket == "atlantean-land-415108-dev-data"
    assert _render(dag, sensor.prefix, "20260101") == "demo/customers/20260101/"
    assert sensor.mode == "reschedule"  # doesn't hold a worker slot while it waits
    assert sensor.timeout and sensor.timeout > 0  # gives up instead of waiting forever


def test_load_files_loads_the_days_csv_files_into_the_raw_partition(dag):
    # spec: load_files reads gs://.../demo/customers/{{ ds_nodash }}/*.csv into raw_crm.customer_orders,
    # partitioned by day; source notes: CSV with a header row
    load = dag.get_task("load_files")
    assert load.bucket == "atlantean-land-415108-dev-data"
    assert [_render(dag, s, "20260101") for s in load.source_objects] == ["demo/customers/20260101/*.csv"]
    assert _render(dag, load.destination_project_dataset_table, "20260101") == (
        "atlantean-land-415108.raw_crm.customer_orders$20260101"
    )
    assert load.source_format == "CSV"
    assert load.skip_leading_rows == 1


def test_load_files_schema_has_the_source_columns_all_as_string(dag):
    # spec source notes: columns customer_id, full_name, email, phone, national_id, order_id, amount;
    # assumptions: all loaded as STRING in the raw layer (amount is cast to NUMERIC in gold)
    fields = dag.get_task("load_files").schema_fields
    assert {f["name"] for f in fields} == {
        "customer_id", "full_name", "email", "phone", "national_id", "order_id", "amount",
    }
    assert {f["type"] for f in fields} == {"STRING"}


@pytest.mark.parametrize("step", ["merge_customer_orders", "export_orders"])
def test_sql_steps_run_the_spec_file_as_standard_sql(dag, step):
    # spec: each bigquery_sql step's file is dags/sql/crm_orders_export_daily/<step>.sql
    task = dag.get_task(step)
    query = task.configuration["query"]
    assert f"sql/crm_orders_export_daily/{step}.sql" in query["query"]
    assert query["useLegacySql"] is False
    assert task.project_id == "atlantean-land-415108"
    assert pathlib.Path("dags/sql/crm_orders_export_daily", f"{step}.sql").is_file()


def test_copy_to_s3_copies_the_days_export_to_the_spec_s3_folder(dag):
    # spec: export_orders writes gs://atlantean-land-415108-dev-data/exports/customer_orders/{{ ds_nodash }}/;
    # copy_to_s3 copies it to s3://pa-demo-exports-897729105732/exports/customer_orders/{{ ds_nodash }}/,
    # replacing what is there (write_mode: replace)
    copy = dag.get_task("copy_to_s3")
    assert copy.gcs_bucket == "atlantean-land-415108-dev-data"
    assert _render(dag, copy.prefix, "20260101") == "exports/customer_orders/20260101/"
    # the GCS path is kept under the destination, so files land in exports/customer_orders/<date>/
    assert copy.dest_s3_key == "s3://pa-demo-exports-897729105732/"
    assert copy.keep_directory_structure is True
    assert copy.replace is True


# --- source_prefix (dags/crm_orders_export_daily.py) ---------------------------------------------


def test_source_prefix_gives_the_days_folder():
    assert source_prefix("20260101") == "demo/customers/20260101/"


@pytest.mark.parametrize(
    "bad",
    [
        pytest.param("2026010", id="seven digits"),
        pytest.param("202601011", id="nine digits"),
        pytest.param("", id="empty"),
        pytest.param("2026-01-", id="eight characters but not all digits"),
        pytest.param("2026-01-01", id="a date with dashes"),
    ],
)
def test_source_prefix_rejects_anything_but_yyyymmdd(bad):
    with pytest.raises(ValueError):
        source_prefix(bad)


# spec: "One folder per day, YYYYMMDD" under gs://atlantean-land-415108-dev-data/demo/customers/
@given(st.dates(min_value=datetime.date(2000, 1, 1), max_value=datetime.date(9999, 12, 31)))
def test_source_prefix_maps_every_day_to_its_own_folder(day):
    ds_nodash = day.strftime("%Y%m%d")
    assert source_prefix(ds_nodash) == f"demo/customers/{ds_nodash}/"
