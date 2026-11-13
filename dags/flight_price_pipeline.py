"""Flight-price ETL: CSV -> MySQL staging -> Spark -> PostgreSQL star schema."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import chain, dag, get_current_context, task
from sqlalchemy import text

# Airflow only puts dags/ on the path; the project root is needed for `include`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.config import (
    ALERT_EMAILS,
    ALERT_FROM_EMAIL,
    ANALYTICS_SCHEMA,
    COLUMN_MAP,
    CSV_PATH,
    MAX_REJECTED_RATIO,
    MYSQL_CONN_ID,
    PG_CONN_ID,
    PROJECT_ROOT,
    REQUIRED_COLUMNS,
    SMTP_CONN_ID,
    STAGING_COLUMNS,
    STAGING_TABLE,
    TASK_RETRIES,
    TASK_RETRY_DELAY_MINUTES,
    WORK_DIR,
)

STAGING_SQL = PROJECT_ROOT / "include" / "sql" / "create_staging_mysql.sql"
ANALYTICS_SQL = PROJECT_ROOT / "include" / "sql" / "create_analytics_postgres.sql"
STAR_TABLES = ["dim_airline", "dim_route", "dim_season", "fact_flight_prices"]

log = logging.getLogger(__name__)


def read_sql_statements(path: Path) -> list[str]:
    return [part.strip() for part in path.read_text(encoding="utf-8").split(";") if part.strip()]


def current_run_id() -> str:
    run_id = get_current_context().get("run_id")
    if run_id is None:
        raise RuntimeError("No run_id in the task context")
    return run_id


def run_folder(run_id: str) -> Path:
    safe_id = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)
    return WORK_DIR / safe_id


def failure_alert() -> SmtpNotifier | None:
    """Email sent when a task fails for good (Airflow only calls it after the last retry)."""
    if not ALERT_EMAILS:
        return None
    return SmtpNotifier(
        smtp_conn_id=SMTP_CONN_ID,
        from_email=ALERT_FROM_EMAIL or None,
        to=ALERT_EMAILS,
        subject="[Airflow] {{ dag.dag_id }}.{{ ti.task_id }} failed",
        html_content=(
            "<p>Task <b>{{ ti.task_id }}</b> in DAG <b>{{ dag.dag_id }}</b> failed "
            "after {{ ti.try_number }} attempt(s).</p>"
            "<ul>"
            "<li>Run: {{ run_id }}</li>"
            "<li>Error: {{ exception }}</li>"
            "<li>Logs: <a href=\"{{ ti.log_url }}\">{{ ti.log_url }}</a></li>"
            "</ul>"
        ),
    )


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(minutes=TASK_RETRY_DELAY_MINUTES),
    # Waits 2, 4, ... minutes between tries, capped so a broken run does not hang around.
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}
if (alert := failure_alert()) is not None:
    DEFAULT_ARGS["on_failure_callback"] = alert


@dag(
    dag_id="flight_price_pipeline",
    description="Stage, validate, transform with Spark, and load a flight-price star schema",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flight", "etl", "spark", "star-schema"],
    default_args=DEFAULT_ARGS,
)
def flight_price_pipeline():
    @task
    def ensure_staging_table() -> None:
        """Create the MySQL staging table if it does not exist."""
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        for statement in read_sql_statements(STAGING_SQL):
            hook.run(statement)
        # Older versions of the table were created without the datetime columns.
        existing = {row[0] for row in hook.get_records(f"SHOW COLUMNS FROM {STAGING_TABLE}")}
        for column in ("departure_datetime", "arrival_datetime"):
            if column not in existing:
                hook.run(f"ALTER TABLE {STAGING_TABLE} ADD COLUMN {column} VARCHAR(50)")

    @task
    def ingest_csv() -> str:
        """Full-refresh load of the CSV into MySQL staging."""
        if not CSV_PATH.is_file():
            raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

        df = pd.read_csv(CSV_PATH)
        missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Optional source columns that are absent become NULL columns.
        staging = df.rename(columns=COLUMN_MAP).reindex(columns=STAGING_COLUMNS)
        for column in ("base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt", "duration_hrs"):
            staging[column] = pd.to_numeric(staging[column], errors="coerce")
        staging["days_before_departure"] = pd.to_numeric(
            staging["days_before_departure"], errors="coerce"
        ).astype("Int64")

        engine = MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql(f"DELETE FROM {STAGING_TABLE}")
            staging.to_sql(
                "stg_flight_prices", conn, schema="flight_staging",
                if_exists="append", index=False, method="multi", chunksize=500,
            )
            staged = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {STAGING_TABLE}").scalar_one()
            if staged != len(staging):
                raise RuntimeError(f"Staged {staged} rows but the CSV has {len(staging)}")

        log.info("Staged %s rows into %s", staged, STAGING_TABLE)
        return str(run_folder(current_run_id()))

    @task
    def validate_data(work_dir: str) -> str:
        """Extract staged rows from MySQL and profile data quality with Spark."""
        from include.scripts.spark_transform import (
            create_spark_session,
            validate_staging,
        )

        folder = Path(work_dir)
        folder.mkdir(parents=True, exist_ok=True)
        engine = MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_sqlalchemy_engine()
        staging = pd.read_sql(f"SELECT {', '.join(STAGING_COLUMNS)} FROM {STAGING_TABLE}", engine)
        if staging.empty:
            raise ValueError("The staging table is empty")
        staging.to_parquet(folder / "staging.parquet", index=False)

        spark = create_spark_session()
        try:
            report = validate_staging(spark.read.parquet(str(folder / "staging.parquet")))
        finally:
            spark.stop()

        (folder / "quality_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        log.info("Data-quality report: %s", report)
        return work_dir

    @task
    def transform_to_star_schema(work_dir: str) -> str:
        """Clean rows, quarantine rejects, and build the star-schema tables."""
        from include.scripts.spark_transform import (
            build_star_tables,
            clean,
            create_spark_session,
            write_parquet,
        )

        folder = Path(work_dir)
        spark = create_spark_session()
        try:
            valid, rejected = clean(spark.read.parquet(str(folder / "staging.parquet")))
            tables = {**build_star_tables(valid), "rejected_rows": rejected}
            write_parquet(tables, folder / "star")
            loaded_rows = tables["fact_flight_prices"].count()
            rejected_rows = rejected.count()
        finally:
            spark.stop()

        report_path = folder / "quality_report.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report.update(valid_rows=loaded_rows, rejected_rows=rejected_rows)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        log.info("Valid rows: %s, rejected rows: %s", loaded_rows, rejected_rows)

        if rejected_rows / report["input_rows"] > MAX_REJECTED_RATIO:
            raise ValueError(
                f"{rejected_rows} of {report['input_rows']} rows rejected, above the "
                f"{MAX_REJECTED_RATIO:.0%} limit. See {folder / 'star' / 'rejected_rows'}"
            )
        return work_dir

    @task
    def load_to_postgres(work_dir: str) -> None:
        """Full-refresh load of the star schema and a run-log entry, in one transaction."""
        folder = Path(work_dir)
        report = json.loads((folder / "quality_report.json").read_text(encoding="utf-8"))
        engine = PostgresHook(postgres_conn_id=PG_CONN_ID).get_sqlalchemy_engine()

        with engine.begin() as conn:
            for statement in read_sql_statements(ANALYTICS_SQL):
                conn.exec_driver_sql(statement)
            tables = ", ".join(f"{ANALYTICS_SCHEMA}.{name}" for name in STAR_TABLES)
            conn.exec_driver_sql(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE")
            # Dimensions first so the fact's foreign keys are satisfied.
            for name in STAR_TABLES:
                frame = pd.read_parquet(folder / "star" / name)
                # Decimal objects are sent as text by psycopg 3; NUMERIC(12,2) rounds the floats back.
                fares = [column for column in frame.columns if column.endswith("_bdt")]
                frame[fares] = frame[fares].astype("float64")
                frame.to_sql(
                    name, conn, schema=ANALYTICS_SCHEMA,
                    if_exists="append", index=False, method="multi", chunksize=500,
                )

            loaded = conn.exec_driver_sql(
                f"SELECT COUNT(*) FROM {ANALYTICS_SCHEMA}.fact_flight_prices"
            ).scalar_one()
            if loaded != report["valid_rows"]:
                raise RuntimeError(f"Loaded {loaded} fact rows, expected {report['valid_rows']}")

            conn.execute(
                text(
                    f"INSERT INTO {ANALYTICS_SCHEMA}.etl_run_log (run_id, input_rows, loaded_rows, "
                    "rejected_rows, total_fare_recomputed_rows) "
                    "VALUES (:run_id, :input_rows, :loaded_rows, :rejected_rows, :recomputed)"
                ),
                {
                    "run_id": current_run_id(),
                    "input_rows": report["input_rows"],
                    "loaded_rows": loaded,
                    "rejected_rows": report["rejected_rows"],
                    "recomputed": report["total_fare_recomputed_rows"],
                },
            )
        log.info("Loaded %s fact rows into %s", loaded, ANALYTICS_SCHEMA)

    work_dir = ingest_csv()
    chain(ensure_staging_table(), work_dir)
    # At parse time each task returns an XComArg that Airflow resolves to a str at run time,
    # but Airflow's type hints don't model that, so Pylance flags it.
    load_to_postgres(transform_to_star_schema(validate_data(work_dir)))  # pyright: ignore[reportArgumentType]


flight_price_pipeline()
