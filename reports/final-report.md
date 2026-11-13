# Flight Price Analysis - Project Report

Name: Fabrice Sheja
Date: 13 November 2026
Project: Airflow data pipeline for the Bangladesh flight price dataset

## 1. Summary

For this project I built a pipeline in Apache Airflow that takes the Bangladesh
flight price CSV from Kaggle (57,000 rows), loads it into a MariaDB (MySQL) staging table,
checks and cleans it with PySpark, and loads a star schema into PostgreSQL.
The four KPIs from the brief are SQL views in PostgreSQL.

Some results I found:

- Average fares in peak seasons (Eid, Hajj and Winter Holidays) were about
  20% higher than in the rest of the year: 81,093 BDT compared with
  67,337 BDT. Hajj was the most expensive season, at 96,190 BDT on average.
- Average fares were similar across airlines. The highest was Turkish
  Airlines (74,739 BDT) and the lowest was Vistara (67,498 BDT).
- US-Bangla Airlines had 4,496 rows, almost twice as many as any other
  airline.
- No rows had to be dropped. However, in 2,522 rows the Total Fare in the CSV
  was not equal to Base Fare + Tax & Surcharge, so I recalculated it.

## 2. Architecture

The pipeline has five Airflow tasks that run one after the other:

```text
CSV file
   |
   v
ensure_staging_table       create the MariaDB staging table
   |
   v
ingest_csv                 CSV -> MariaDB staging (raw copy)
   |
   v
validate_data              MariaDB staging -> Spark -> quality report (JSON)
   |
   v
transform_to_star_schema   Spark -> Parquet star tables + rejected rows
   |
   v
load_to_postgres           Parquet -> PostgreSQL "analytics" schema + run log
```

Everything runs in Docker Compose. I split the containers by job, the same
way a cloud setup splits orchestration, storage and compute into separate
services:

```text
                 +-------------------------------+
                 |  Airflow (orchestration)      |
                 |  api-server, scheduler,       |
                 |  dag-processor, triggerer     |
                 |  Pandas + Spark driver        |
                 +---+-----------+-----------+---+
                     |           |           |
        staging      |           | Spark     |  warehouse
                     v           v jobs      v
            +----------+  +--------------+  +---------------------+
            | mariadb  |  | spark-master |  | postgres            |
            | staging  |  |      |       |  | Airflow metadata +  |
            | database |  | spark-worker |  | "analytics" schema  |
            +----------+  +--------------+  +---------------------+
                                 |
                  shared folder: include/data (Parquet files)
```

- **Airflow** decides what runs and when. The tasks use Pandas for the
  small database reads and writes, and start the Spark jobs. The Spark
  "driver" (the part that plans the job) runs inside the Airflow scheduler
  container.
- **spark-master and spark-worker** are a small Spark cluster. The master
  hands the work to the worker, and the worker's executors do the actual
  reading, cleaning and writing of the data. To get more compute I would add
  more workers, without touching Airflow.
- **mariadb** (MariaDB 11.8, which speaks the MySQL protocol) holds the
  staging database `flight_staging`.
- **postgres** (Postgres 16) stores the Airflow metadata. The star schema is
  in a separate `analytics` schema in the same database, so my tables don't
  mix with Airflow's own tables.
- The Airflow and Spark containers share the `include/data` folder. Parquet
  files written by one side can be read by the other. Each run uses its own
  folder, `include/data/processed/<run_id>/`, so runs don't overwrite each
  other's files.

How this maps to cloud services (for example on AWS or GCP):

| In this project | Cloud equivalent |
| --- | --- |
| Airflow containers | Managed Airflow (MWAA, Cloud Composer) |
| spark-master + spark-worker | Managed Spark cluster (EMR, Dataproc) |
| Shared `include/data` folder | Object storage (S3, GCS) |
| mariadb | Managed MySQL/MariaDB (RDS, Cloud SQL) |
| postgres `analytics` schema | Warehouse database (RDS PostgreSQL, Redshift) |
| `.env` passwords | Secrets manager |

The WSL setup does not have a cluster. There Spark runs in "local" mode
inside the Airflow process. The same code works in both because the Spark
master address comes from the `SPARK_MASTER` setting.

## 3. Why I used each tool

**Airflow** runs the tasks in the right order, retries a task once if it
fails, and gives me logs and a UI. I set `schedule=None` because the data is a
fixed Kaggle file, so it makes more sense to trigger the DAG by hand than on a
timer. `max_active_runs=1` stops two runs from loading at the same time.

**Pandas** moves data in and out of the databases: CSV into MariaDB, MariaDB
staging out to Parquet for Spark, and the Spark output into PostgreSQL. It
works with the SQLAlchemy engines that the Airflow MySQL and Postgres hooks
return, so each load is a `to_sql()` or `read_sql()` call. Setting up Spark
JDBC drivers would have been more work for 57k rows.

**PySpark** does the validation and transformation. The dataset is small
enough for Pandas alone, but I used Spark because the brief asked for it and
because the same code would work on a much bigger dataset. I convert the
fares with `try_cast`, so a bad value becomes NULL instead of crashing the job.

**MariaDB** (a MySQL-compatible database) is the staging area. Airflow
connects to it with the normal MySQL hook. It keeps a copy of the raw data in the same
shape as the CSV, and the Spark tasks read from it (not from the CSV), so
staging really is the input to the rest of the pipeline.

**PostgreSQL** is the analytics database. I used it for the star schema
because it supports foreign keys, CHECK constraints, `TRUNCATE ... CASCADE`
and window functions, which I needed for ranking the routes.

**SQL** is used for the table definitions (keys and constraints) and for the
KPI views. I put the KPIs in views so they are always calculated from
whatever data is currently in the tables.

**Parquet** is the format for passing data between the Spark tasks and the
load task. It keeps the column types, and if a later task fails it can be
retried without rerunning the earlier ones.

## 4. The DAG and its tasks

DAG id: `flight_price_pipeline`. Each task retries once after 1 minute.
Manual trigger, no catchup.

### ensure_staging_table

Runs `create_staging_mysql.sql` to create `stg_flight_prices` if it doesn't
exist. It also checks the columns, and if `departure_datetime` or
`arrival_datetime` are missing (from an older version of my table) it adds
them with `ALTER TABLE`.

### ingest_csv

Reads the CSV with Pandas and checks that the six required columns are there:
Airline, Source, Destination, Base Fare, Tax & Surcharge and Total Fare. It
renames the columns to snake_case (optional columns that are missing become
empty columns) and converts the numeric columns. Then, in a
single transaction, it deletes the old staging rows, inserts the new ones in
batches of 500, and checks that the number of rows in MariaDB matches the CSV.
If the counts don't match, the transaction is rolled back.

### validate_data

Reads the staged rows back from MariaDB, saves them as Parquet, and uses Spark
to count data quality problems:

- blank airline, source or destination
- airport codes that are not 3 letters
- missing base fare or tax
- negative base fare or tax
- rows where total fare is not base + tax (allowing a 0.01 difference)

If the staging table is empty or a column is missing, the task fails.
Otherwise it only counts the problems and saves them to `quality_report.json`.

### transform_to_star_schema

This is where the cleaning happens:

1. Trim the text fields and make airport codes uppercase.
2. Cast the fares to DECIMAL(12,2). If seasonality is empty, set it to "Unknown".
3. Split the rows into valid and rejected. A row is rejected if it has a
   missing airline, source or destination, an invalid code, or a missing or
   negative base fare or tax.
4. Recalculate `total_fare_bdt = base_fare_bdt + tax_surcharge_bdt` and turn
   the departure and arrival text into real timestamps.
5. Build the three dimension tables, with keys from `row_number()` ordered by
   name. This means the same input always produces the same keys.
6. Join the keys onto the cleaned rows to make the fact table.

The tables are written as Parquet. Rejected rows are not thrown away: they
are saved to a `rejected_rows` Parquet folder with a `reject_reason` column
(for example "negative base or tax fare"), so I can see why each one failed. If
more than 5% of the rows are rejected (`MAX_REJECTED_RATIO`), the task fails,
because that usually means something is wrong with the source file.

### load_to_postgres

In one transaction, this task runs the PostgreSQL DDL, truncates the four
tables (`RESTART IDENTITY CASCADE`), loads the dimensions first and then the
fact table (because of the foreign keys), and checks that the fact row count
matches what Spark produced. It then adds a row to `analytics.etl_run_log`
with the run ID and the input, loaded, rejected and recalculated row counts.
If anything fails, everything rolls back.

### Re-running the pipeline

Each run deletes and reloads everything (a full refresh), so running it twice
gives the same result instead of duplicating rows. Both database loads are
inside transactions, so a failed run never leaves a table half-loaded.

## 5. Star schema

**Grain:** one row in `fact_flight_prices` for each valid row in the CSV.

**Dimensions:**

| Table | Natural key | Surrogate key | Other columns | Rows |
| --- | --- | --- | --- | --- |
| dim_airline | airline | airline_key | - | 24 |
| dim_route | source + destination | route_key | source_name, destination_name | 152 |
| dim_season | seasonality | season_key | is_peak | 4 |

Each natural key has a UNIQUE constraint.

**Measures** in the fact table: `base_fare_bdt`, `tax_surcharge_bdt` and
`total_fare_bdt`. All three are additive. PostgreSQL CHECK constraints make
sure the fares are not negative and that total = base + tax. The fact table
also stores some descriptive columns (duration, stopovers, aircraft type,
class, booking source, days before departure, and departure and arrival time
as `TIMESTAMP`). I left these on the fact table because they describe that one
observation only.

All warehouse tables live in the `analytics` schema. There is also an
`etl_run_log` table that keeps one row per pipeline run for auditing.

**Route dimension:** I kept the airport names directly in `dim_route` and did
not make a separate airport table. There are only 152 routes, and an extra
table would only add another join.

**Bridge tables:** I didn't use any. A bridge table is for many-to-many
relationships, but in this dataset every row has exactly one airline, one
route and one season, so a bridge table would only duplicate data. If the
data had, for example, several airlines on one ticket (codeshare), then a
bridge table would make sense.

**Booking count:** the dataset doesn't have a booking ID, so "booking count"
in this project means the number of rows (observations), not real tickets
sold.

## 6. KPIs

All of these are views in the `analytics` schema, defined in
`create_analytics_postgres.sql`. Averages are rounded to 2 decimals. The
numbers below come from querying the views after a full DAG run.

### Average fare by airline (`vw_avg_fare_by_airline`)

`AVG(total_fare_bdt)` grouped by airline.

| Airline | Avg total fare (BDT) | Rows |
| --- | ---: | ---: |
| Turkish Airlines | 74,738.63 | 2,220 |
| AirAsia | 73,830.16 | 2,312 |
| Cathay Pacific | 72,594.05 | 2,282 |
| Malaysian Airlines | 72,247.17 | 2,292 |
| Thai Airways | 72,062.79 | 2,316 |
| ... | ... | ... |
| NovoAir | 67,833.99 | 2,268 |
| Singapore Airlines | 67,556.07 | 2,279 |
| Vistara | 67,497.63 | 2,368 |

### Seasonal fare variation (`vw_seasonal_fare_variation`, `vw_peak_vs_non_peak_fare`)

`AVG(total_fare_bdt)` grouped by season, then grouped by the `is_peak` flag.
I counted Eid, Hajj and Winter Holidays as peak seasons.

| Season | Peak | Avg total fare (BDT) | Rows |
| --- | --- | ---: | ---: |
| Hajj | yes | 96,189.95 | 942 |
| Eid | yes | 90,790.87 | 603 |
| Winter Holidays | yes | 79,256.33 | 10,930 |
| Regular | no | 67,337.33 | 44,525 |
| All peak | yes | 81,092.55 | 12,475 |
| Non-peak | no | 67,337.33 | 44,525 |

### Booking count by airline (`vw_booking_count_by_airline`)

`COUNT(*)` grouped by airline. US-Bangla Airlines is first with 4,496 rows.
The other 23 airlines have between 2,201 and 2,368 rows each.

### Most popular routes (`vw_most_popular_routes`)

`COUNT(*)` grouped by route, ranked with `ROW_NUMBER()`. I ordered ties by
source and destination so the ranking is the same every time.

| Rank | Route | Rows |
| ---: | --- | ---: |
| 1 | RJH -> SIN (Rajshahi - Singapore) | 417 |
| 2 | DAC -> DXB (Dhaka - Dubai) | 413 |
| 3 | BZL -> YYZ (Barisal - Toronto) | 410 |
| 4 | CGP -> BKK (Chittagong - Bangkok) | 408 |
| 5 | CXB -> DEL (Cox's Bazar - Delhi) | 408 |
| 6 | BZL -> JED (Barisal - Jeddah) | 407 |
| 7 | CGP -> CXB (Chittagong - Cox's Bazar) | 404 |
| 8 | RJH -> JFK (Rajshahi - New York) | 404 |
| 9 | SPD -> DEL (Saidpur - Delhi) | 404 |
| 10 | CGP -> RJH (Chittagong - Rajshahi) | 403 |

The route counts are very even: the 1st route has only 14 more rows than the
10th. I think this is because the dataset is synthetic.

## 7. Data quality results

| Check | Result |
| --- | --- |
| Required columns present | Yes |
| Blank airline / source / destination | 0 |
| Invalid airport codes | 0 |
| Non-numeric or negative fares | 0 |
| Total fare not equal to base + tax | 2,522 (recalculated) |
| Rows in CSV / staging / fact table | 57,000 / 57,000 / 57,000 |
| Rows rejected | 0 |

The same numbers are saved in `analytics.etl_run_log` for every run, so I can
check them with SQL instead of reading the Airflow logs.

The 2,522 mismatched rows were off by between 445 and 93,165 BDT, so these
were not rounding errors. The brief says Total Fare = Base Fare + Tax &
Surcharge, so I went with that and recalculated the total.

## 8. Challenges and how I solved them

**Winter wasn't being counted as peak season.** My config had
`PEAK_SEASONS = {"Eid", "Hajj", "Winter"}`, but in the real CSV the value is
"Winter Holidays". My small test file used "Winter", so the tests passed even
though the real data was wrong. Because of this, the peak average covered only
Eid and Hajj (1,545 rows). I added "Winter Holidays" to the set, and now 12,475
rows are counted as peak.

**Total fare didn't add up in some rows.** Explained in section 7. I count
these rows during validation, recalculate them during the transform, and a
CHECK constraint in PostgreSQL stops a wrong total from being loaded.

**MySQL staging wasn't really used.** In my first version, `ingest_csv` loaded
the CSV into MySQL, but then the Spark tasks read the CSV file again. That
meant the staging table was a dead end, and a problem in the MySQL load would
never affect the rest of the pipeline. I changed `validate_data` to read the
rows back from MySQL, so everything after ingestion uses the staged data.

**My tables were mixed with Airflow's tables.** The star schema was in the
`public` schema of the same database Airflow uses for its metadata, so my
tables sat next to Airflow's internal ones. I moved everything into its own
`analytics` schema.

**Bad rows disappeared without a trace.** Rows that failed the checks were
just filtered out. Now they are saved as `rejected_rows` with the reason they
failed, and the run fails if more than 5% of the rows are rejected.

**Duplicate code.** I had written the validation and KPIs in Pandas first
(`validate.py` and `transform.py`) and later rewrote them in Spark and SQL, but
I never deleted the Pandas versions. Most of my tests were testing that
unused code. I deleted it and wrote tests for the Spark functions the DAG
actually runs, plus a test that checks the DAG loads and the tasks are in the
right order.

**The staging table was missing columns.** I had created the MySQL table
before adding the datetime columns. `CREATE TABLE IF NOT EXISTS` doesn't
change a table that already exists, so the new columns were never added. I
added a check that runs `ALTER TABLE` when they are missing.

**Spark wouldn't run in Airflow.** The official Airflow image has no Java and
no PySpark, so the Spark tasks failed with "No module named pyspark" and
then a JVM error. I made my own Dockerfile that installs OpenJDK 21 and sets
`JAVA_HOME`, and added PySpark to `requirements.txt`.

**Docker worked differently from WSL.** My Docker setup still used Airflow 2
commands (`airflow users create`), which don't exist in Airflow 3, so there was
no way to log in. I switched to Airflow 3's simple auth manager with the user
and password from `.env`. Then the load failed only in Docker: the image uses
psycopg 3, which sent the decimal fares as text. I convert the fares to float
before loading, and PostgreSQL rounds them back to NUMERIC(12,2).

**The Spark cluster couldn't write to the shared folder.** When I moved Spark
into its own containers, the job failed with "Mkdirs failed". Airflow created
the run folder as its own user (uid 50000), but the Spark image runs as a
different user (uid 185) that wasn't allowed to write there. Running the Spark
image as uid 50000 made Hadoop crash because that user didn't exist inside
the image. I added a small `Dockerfile.spark` that creates the user and runs
Spark as it, so both sides write as the same user. In the cloud, this would
be giving both services permission on the same storage bucket.

**Spark couldn't write Parquet on Windows.** On Windows, Hadoop needs
`winutils.exe`. Instead of setting that up, I run the pipeline in Docker or
WSL.

**Re-running caused duplicate or half-loaded data.** My early versions just
appended the data, so every rerun added the rows again. Now each load deletes
or truncates the old data and inserts the new data in one transaction, and
checks the row count before committing.

**No booking ID.** The KPIs ask for a booking count, but the data has no
booking ID. I count rows instead and explained this in the report.

## 9. How to run it

With Docker:

```bash
cp .env.example .env            # set the passwords
mkdir -p logs plugins config
docker compose up -d --build
# open http://localhost:8080, log in with the user and password from .env,
# and trigger flight_price_pipeline
# Spark master UI: http://localhost:9090
```

Or in WSL without Docker: `bash scripts/setup_wsl.sh` once, then
`bash scripts/run_dag.sh`. A full run takes about 1.5 minutes.

Then in PostgreSQL:

```sql
SELECT * FROM analytics.vw_peak_vs_non_peak_fare;
SELECT * FROM analytics.vw_most_popular_routes WHERE route_rank <= 10;
SELECT * FROM analytics.etl_run_log;
```

Tests: `pytest -q` runs 3 tests for the Spark logic and 3 for the DAG. There is
also an end-to-end test that runs the whole DAG twice against the real
databases and checks that the row counts match the CSV and do not double on
the second run. It needs the databases, so it only runs with
`RUN_INTEGRATION=1`. All 7 tests pass.

## 10. What I would add next

- Load only new data each run instead of reloading everything.
- Add a date dimension so I can look at prices by month or day of the week.
- Fix the PySpark warning about pandas 3 (pin pandas below 3 or upgrade PySpark).

## Files

- DAG: `dags/flight_price_pipeline.py`
- Spark code: `include/scripts/spark_transform.py`
- MariaDB staging table: `include/sql/create_staging_mysql.sql`
- PostgreSQL tables and KPI views: `include/sql/create_analytics_postgres.sql`
- Tests: `tests/`
