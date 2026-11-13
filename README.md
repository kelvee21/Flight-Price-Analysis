# Flight Price Analysis - Airflow Data Pipeline

An Apache Airflow pipeline that loads the Bangladesh flight price CSV into a
**MySQL** staging table, validates and transforms it with **PySpark**, and loads
a **PostgreSQL** star schema with SQL views for the KPIs.

The full write-up is in [`reports/final-report.md`](reports/final-report.md)
(also as [`final-report.docx`](reports/final-report.docx)).

## Pipeline

```text
ensure_staging_table -> ingest_csv -> validate_data -> transform_to_star_schema -> load_to_postgres
```

| Task | What it does |
| --- | --- |
| `ensure_staging_table` | Creates `flight_staging.stg_flight_prices` in MySQL |
| `ingest_csv` | Pandas loads the CSV into staging (full refresh, row count checked) |
| `validate_data` | Reads staging from MySQL and counts data-quality problems with Spark |
| `transform_to_star_schema` | Spark cleans rows, quarantines rejects, builds dimensions and the fact |
| `load_to_postgres` | Loads the `analytics` schema in one transaction and writes a run-log row |

## Project layout

```text
.
├── dags/flight_price_pipeline.py     # Airflow DAG
├── include/
│   ├── config.py                     # settings, column mapping, peak seasons
│   ├── scripts/spark_transform.py    # Spark validation and star-schema build
│   ├── sql/                          # MySQL staging + PostgreSQL warehouse DDL and KPI views
│   └── data/                         # source CSV (Spark output goes to data/processed/, ignored)
├── tests/                            # pytest: Spark logic and DAG structure
├── scripts/                          # WSL setup and run helpers
├── reports/                          # final project report (.md and .docx)
├── Dockerfile                        # Airflow + Java 21 + PySpark image
└── docker-compose.yaml               # Airflow + MySQL + PostgreSQL
```

## Star schema (`analytics` schema in PostgreSQL)

- `fact_flight_prices`: one row per valid CSV row, with base, tax and total fare
- `dim_airline`, `dim_route`, `dim_season` (with an `is_peak` flag)
- `etl_run_log`: one row per load with input, loaded and rejected row counts
- KPI views: `vw_avg_fare_by_airline`, `vw_seasonal_fare_variation`,
  `vw_peak_vs_non_peak_fare`, `vw_booking_count_by_airline`, `vw_most_popular_routes`

There is no booking ID in the data, so "booking count" means the number of
price observations.

## Run with Docker

```bash
cp .env.example .env          # set the passwords
mkdir -p logs plugins config
docker compose up -d --build
# open http://localhost:8080, log in with _AIRFLOW_WWW_USER_USERNAME / _PASSWORD from .env,
# and trigger flight_price_pipeline
```

## Run in WSL (no Docker)

```bash
cp .env.example .env
bash scripts/setup_wsl.sh     # MySQL, Java, venv, Airflow DB, connections
bash scripts/run_dag.sh       # runs the whole DAG once
```

## Check the results

```sql
SELECT * FROM analytics.vw_peak_vs_non_peak_fare;
SELECT * FROM analytics.vw_most_popular_routes WHERE route_rank <= 10;
SELECT * FROM analytics.etl_run_log ORDER BY loaded_at DESC LIMIT 1;
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

The DAG tests are skipped when Airflow is not installed. The end-to-end test
runs the whole DAG twice against the real databases, so it only runs on request:

```bash
RUN_INTEGRATION=1 pytest tests/test_pipeline_integration.py
```

Rejected rows are kept in `include/data/processed/<run_id>/star/rejected_rows`
with a `reject_reason` column.

## Data

Kaggle: [Flight Price Dataset of Bangladesh](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)
by Mahatir Ahmed Tusher, CC BY-SA 4.0. See [`include/data/README.md`](include/data/README.md).
