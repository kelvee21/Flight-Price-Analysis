from itertools import pairwise

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag

from include.config import PROJECT_ROOT


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
    assert all(task.retries >= 1 for task in dag.tasks)
