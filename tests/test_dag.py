import importlib
from itertools import pairwise

import pytest

from include.config import PROJECT_ROOT

pytest.importorskip("airflow")
# Airflow's DAG parsing needs fcntl, which only exists on Linux/macOS (run this in Docker).
pytest.importorskip("fcntl")

from airflow.models import DagBag  # noqa: E402  (only import once we know Airflow can run)


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(PROJECT_ROOT / "dags"))
    assert bag.import_errors == {}
    return bag.get_dag("flight_price_pipeline")


def test_dag_has_expected_tasks(dag):
    assert set(dag.task_ids) == {
        "ensure_staging_table",
        "ingest_csv",
        "validate_data",
        "transform_to_star_schema",
        "load_to_postgres",
    }


def test_tasks_run_in_order(dag):
    order = ["ensure_staging_table", "ingest_csv", "validate_data",
             "transform_to_star_schema", "load_to_postgres"]
    for upstream, downstream in pairwise(order):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_tasks_retry(dag):
    for task in dag.tasks:
        assert task.retries >= 2
        assert task.retry_exponential_backoff


def test_failure_alert_is_attached_when_an_email_is_set(monkeypatch):
    import include.config

    monkeypatch.setenv("ALERT_EMAIL", "me@example.com")
    monkeypatch.setenv("GMAIL_USER", "sender@gmail.com")
    importlib.reload(include.config)
    try:
        bag = DagBag(dag_folder=str(PROJECT_ROOT / "dags"))
        assert bag.import_errors == {}
        for task in bag.get_dag("flight_price_pipeline").tasks:
            callbacks = task.on_failure_callback
            callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
            assert any(type(cb).__name__ == "SmtpNotifier" for cb in callbacks)
    finally:
        monkeypatch.undo()
        importlib.reload(include.config)
