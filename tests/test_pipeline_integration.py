"""End-to-end run against real MySQL and PostgreSQL.

Skipped by default. Run it with the databases up and the Airflow env loaded:
    RUN_INTEGRATION=1 pytest tests/test_pipeline_integration.py
"""

import os

import pandas as pd
import pytest

from include.config import CSV_PATH, PG_CONN_ID, PROJECT_ROOT

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 to run against the databases"
)


def run_dag_and_read_warehouse():
    from airflow.models import DagBag
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    dag = DagBag(dag_folder=str(PROJECT_ROOT / "dags")).get_dag("flight_price_pipeline")
    assert dag.test().state == "success"

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    fact_rows = hook.get_first("SELECT COUNT(*) FROM analytics.fact_flight_prices")[0]
    log_row = hook.get_first(
        "SELECT input_rows, loaded_rows, rejected_rows FROM analytics.etl_run_log "
        "ORDER BY run_log_key DESC LIMIT 1"
    )
    peak_rows = hook.get_first("SELECT SUM(booking_count) FROM analytics.vw_peak_vs_non_peak_fare")[0]
    return fact_rows, tuple(log_row), peak_rows


def test_full_run_loads_warehouse_and_is_idempotent():
    csv_rows = len(pd.read_csv(CSV_PATH))

    first = run_dag_and_read_warehouse()
    fact_rows, (input_rows, loaded_rows, rejected_rows), peak_rows = first

    assert input_rows == csv_rows
    assert loaded_rows == fact_rows == peak_rows
    assert loaded_rows + rejected_rows == csv_rows

    # A second run must give the same warehouse, not doubled rows.
    assert run_dag_and_read_warehouse() == first
