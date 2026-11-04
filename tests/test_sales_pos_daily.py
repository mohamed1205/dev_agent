"""DAG structure tests for pipeline sales-pos-daily (pipelines/sales-pos-daily/pipeline.yaml).

What is expected here comes from the spec, the conventions (.platform-agents/conventions.yaml) and the
business rules (business_logic.md), not from the DAG file. Nothing here reaches GCS, BigQuery or S3.
"""
import datetime
import pathlib

import pytest
from airflow.models import DagBag

DAG_ID = "sales_pos_daily"

# pipelines/sales-pos-daily/pipeline.yaml: steps, in the spec's order
SPEC_STEPS = [
    "wait_for_ruh",
    "wait_for_jed",
    "wait_for_dmm",
    "wait_for_quiet",
    "load_files",
    "validate_raw",
    "merge_transactions",
    "build_daily_sales",
    "check_transactions",
    "check_daily_sales",
    "stage_export",
    "copy_export_to_s3",
]

# pipelines/sales-pos-daily/pipeline.yaml: the SQL file each SQL step runs (`file:`), relative to dags/
SPEC_SQL_FILES = {
    "validate_raw": "sql/sales_pos_daily/validate_raw.sql",
    "merge_transactions": "sql/sales_pos_daily/merge_transactions.sql",
    "build_daily_sales": "sql/sales_pos_daily/build_daily_sales.sql",
    "check_transactions": "sql/sales_pos_daily/check_transactions.sql",
    "check_daily_sales": "sql/sales_pos_daily/check_daily_sales.sql",
    "stage_export": "sql/sales_pos_daily/stage_export.sql",
}

BUCKET = "atlantean-land-415108-dev-data"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags[DAG_ID]


def ancestors(dag, task_id):
    """Every task that must finish before task_id runs."""
    seen, todo = set(), [task_id]
    while todo:
        for up in dag.get_task(todo.pop()).upstream_task_ids:
            if up not in seen:
                seen.add(up)
                todo.append(up)
    return seen


# ---------------------------------------------------------------------------------------------- steps
def test_the_dag_has_exactly_the_spec_steps(dag):
    assert sorted(dag.task_ids) == sorted(SPEC_STEPS)


def test_each_branch_sensor_feeds_the_quiet_period_wait(dag):
    # spec: at least one file per branch, then no new upload for 5 minutes
    assert dag.get_task("wait_for_quiet").upstream_task_ids == {"wait_for_ruh", "wait_for_jed", "wait_for_dmm"}


def test_files_are_loaded_only_after_every_wait(dag):
    assert {"wait_for_ruh", "wait_for_jed", "wait_for_dmm", "wait_for_quiet"} <= ancestors(dag, "load_files")
    assert dag.get_task("load_files").upstream_task_ids == {"wait_for_quiet"}


def test_validate_raw_runs_right_after_the_load(dag):
    assert dag.get_task("validate_raw").upstream_task_ids == {"load_files"}


def test_nothing_reaches_the_sales_dataset_or_aws_before_validate_raw(dag):
    # spec, validate_raw: "Fails the run before anything reaches the sales dataset"
    for writer in ["merge_transactions", "build_daily_sales", "stage_export", "copy_export_to_s3"]:
        assert "validate_raw" in ancestors(dag, writer), writer


def test_steps_follow_their_inputs(dag):
    # Each step runs after the step that writes the table or files it reads (the spec's inputs and outputs).
    assert "merge_transactions" in ancestors(dag, "build_daily_sales")
    assert "merge_transactions" in ancestors(dag, "check_transactions")
    assert {"merge_transactions", "build_daily_sales"} <= ancestors(dag, "check_daily_sales")
    assert "build_daily_sales" in ancestors(dag, "stage_export")
    assert "stage_export" in ancestors(dag, "copy_export_to_s3")


def test_both_gold_checks_pass_before_the_export_to_finance_hq(dag):
    # business rule: Finance HQ's copy has valid branch codes, revenue never null and one row per
    # branch and day; the checks on the gold tables must pass before anything is exported
    for export in ["stage_export", "copy_export_to_s3"]:
        assert {"check_transactions", "check_daily_sales"} <= ancestors(dag, export), export


def test_the_copy_to_s3_is_the_last_step(dag):
    assert dag.get_task("copy_export_to_s3").downstream_task_ids == set()
    assert ancestors(dag, "copy_export_to_s3") == set(SPEC_STEPS) - {"copy_export_to_s3"}


def test_each_sql_step_runs_the_file_the_spec_names(dag):
    for task_id, path in SPEC_SQL_FILES.items():
        task = dag.get_task(task_id)
        sql = task.sql if hasattr(task, "sql") else task.configuration["query"]["query"]
        assert path in sql, task_id
        assert pathlib.Path("dags", path).is_file(), path


def test_check_steps_are_checks_that_fail_the_run(dag):
    # spec: kind check; a BigQueryCheckOperator fails the task when the query's row has a false value
    for task_id in ["validate_raw", "check_transactions", "check_daily_sales"]:
        assert type(dag.get_task(task_id)).__name__ == "BigQueryCheckOperator", task_id


# ------------------------------------------------------------------------------------- schedule, rules
def test_schedule_is_daily_at_0230_utc(dag):
    # spec: schedule "30 2 * * *", timezone UTC
    assert dag.timetable.summary == "30 2 * * *"
    assert dag.timezone.name == "UTC"


def test_a_run_still_going_at_0400_utc_fails(dag):
    # business rule: sales.daily_branch_sales is refreshed every day by 04:00 UTC; the run starts at 02:30
    assert dag.dagrun_timeout is not None
    assert dag.dagrun_timeout <= datetime.timedelta(minutes=90)


def test_conventions(dag):
    # .platform-agents/conventions.yaml defaults, and the spec's owner
    assert dag.owner == "data-platform"
    assert dag.default_args["retries"] == 2
    assert dag.default_args["retry_delay"] == datetime.timedelta(minutes=5)
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert "pipeline:sales-pos-daily" in dag.tags
    assert "owner:data-platform" in dag.tags


# ------------------------------------------------------------------------------------------- sensors
@pytest.mark.parametrize("task_id,branch", [("wait_for_ruh", "ruh"), ("wait_for_jed", "jed"), ("wait_for_dmm", "dmm")])
def test_each_branch_sensor_waits_for_that_branch_in_the_run_day_folder(dag, task_id, branch):
    # spec: gs://atlantean-land-415108-dev-data/pos/{{ ds }}/pos_<branch>_*
    task = dag.get_task(task_id)
    assert task.bucket == BUCKET
    assert task.prefix == f"pos/{{{{ ds }}}}/pos_{branch}_"
    assert task.deferrable is True


def test_quiet_period_needs_three_files_and_five_minutes_without_uploads(dag):
    # spec, wait_for_quiet: "At least 3 files and no new upload for 5 minutes."
    task = dag.get_task("wait_for_quiet")
    assert task.bucket == BUCKET
    assert task.prefix == "pos/{{ ds }}/"
    assert task.min_objects == 3
    assert task.inactivity_period == 5 * 60
    assert task.deferrable is True


# ---------------------------------------------------------------------------------------------- load
def test_load_reads_only_the_run_day_pos_files(dag):
    task = dag.get_task("load_files")
    assert task.bucket == BUCKET
    assert list(task.source_objects) == ["pos/{{ ds }}/pos_*.csv"]


def test_load_replaces_only_the_run_day_partition_of_the_raw_table(dag):
    # spec: raw_pos.pos_transactions, "each run replaces its own partition (the run date)"
    task = dag.get_task("load_files")
    assert task.destination_project_dataset_table == "atlantean-land-415108.raw_pos.pos_transactions${{ ds_nodash }}"
    assert task.write_disposition == "WRITE_TRUNCATE"
    # the table comes from its Flyway migration, never from the load
    assert task.create_disposition == "CREATE_NEVER"


def test_load_reads_the_files_as_csv_with_a_header_and_every_column_as_string(dag):
    # spec, source: "Comma-separated with a header row, all columns read as STRING"
    task = dag.get_task("load_files")
    assert task.source_format == "CSV"
    assert task.skip_leading_rows == 1
    assert {f["type"] for f in task.schema_fields} == {"STRING"}


def test_load_schema_has_the_columns_the_business_rules_name(dag):
    # business_logic.md: transaction_id, amount, currency (SAR), branch_code, transaction_date, refunds
    names = [f["name"] for f in dag.get_task("load_files").schema_fields]
    for column in ["transaction_id", "branch_code", "transaction_date", "transaction_type", "amount", "currency"]:
        assert column in names, column
    assert len(names) == len(set(names))


# --------------------------------------------------------------------------------------------- export
def test_the_s3_copy_takes_only_the_run_day_export_and_overwrites_it(dag):
    # spec: one Parquet file per day under finance/daily_branch_sales/<D>/, same name on a rerun so it is overwritten
    task = dag.get_task("copy_export_to_s3")
    assert task.gcs_bucket == BUCKET
    assert task.prefix == "finance/daily_branch_sales/{{ ds }}/"
    assert task.dest_s3_key == "s3://pa-demo-exports-897729105732/"
    assert task.keep_directory_structure is True
    assert task.replace is True


def test_the_s3_copy_uses_the_keyless_aws_connection(dag):
    # spec: "through Airflow connection aws_default (keyless web identity federation)"
    assert dag.get_task("copy_export_to_s3").dest_aws_conn_id == "aws_default"
