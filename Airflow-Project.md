# Airflow Project: Flight Price Analysis

## Project Overview

Develop an end-to-end data pipeline using Apache Airflow to process and analyze flight price data for Bangladesh. The pipeline must ingest raw CSV data, validate and transform it, compute key performance indicators (KPIs), and store the final results in a PostgreSQL database for further analysis.

---

## Technologies

- **Airflow** — workflow orchestration
- **MySQL** — staging database
- **PostgreSQL** — analytics database
- **Python** — data processing
- **CSV File Source:** Flight Price Dataset of Bangladesh *(Kaggle)*

---

## Pipeline Components & Requirements

### 1. Data Ingestion

- **Input:** `Flight_Price_Dataset_of_Bangladesh.csv`
- **Task:** Load the CSV data into a staging table in MySQL.
- **Validation:** Ensure all data from the CSV is correctly inserted with appropriate column types matching the original structure.

---

### 2. Data Validation

**Checks to Implement:**

- All required columns exist:
  - `Airline`
  - `Source`
  - `Destination`
  - `Base Fare`
  - `Tax & Surcharge`
  - `Total Fare`
- Handle missing or null values.
- Validate data types (e.g., numeric fare values, non-empty strings for categorical fields).
- Flag or correct any inconsistencies (e.g., negative fares or invalid city names).

---

### 3. Data Transformation & KPI Computation

#### Transformation

- If not already present, calculate:

```
Total Fare = Base Fare + Tax & Surcharge
```

#### KPI Metrics to Compute

| KPI | Description |
|---|---|
| **Average Fare by Airline** | Mean total fare grouped by airline |
| **Seasonal Fare Variation** | Compare average fares during peak vs. non-peak seasons (e.g., Eid, Winter holidays) |
| **Booking Count by Airline** | Total number of bookings per airline |
| **Most Popular Routes** | Top source-destination pairs ranked by booking count |

---

### 4. Data Loading into PostgreSQL

- **Task:** Transfer the transformed and enriched data to a PostgreSQL database.
- **Goal:** Ensure data consistency during transfer.

---

### 5. Analytics Data Model

Implement a **star schema** in PostgreSQL:

- `fact_flight_prices`: one row per valid source CSV record; fare amounts are
  additive measures and dimension keys describe each observation.
- `dim_airline`: one row per airline.
- `dim_route`: one row per source/destination pair, with both airport codes
  and names.
- `dim_season`: one row per season label and a peak-season flag.

Do not add bridge tables for this dataset. Each source record contains exactly
one airline, one route, and one season; a bridge would duplicate rows without
representing a real many-to-many relationship. If a future source has multiple
carriers or marketing channels per observation, model that relationship with a
bridge at the appropriate grain.

The source has no booking identifier. Booking-count KPIs are therefore counts
of source records (observation counts), not unique confirmed tickets.

### 6. Tools and responsibility

| Tool | Responsibility |
|---|---|
| Airflow | Orchestrate the ordered, retryable ETL tasks |
| Pandas | Read the CSV header, bulk-load MySQL staging, and load Spark Parquet outputs |
| PySpark | Profile, clean, type, enrich, and build dimensions and the fact |
| MySQL | Raw-shaped staging area |
| PostgreSQL SQL | Enforce warehouse keys/constraints and expose KPI views |

---

## Documentation

A comprehensive report including:

- Pipeline architecture and execution flow
- Description of each Airflow DAG and task
- Star-schema grain, dimensions, measures, keys, and bridge-table decision
- Why Spark, Pandas, SQL, MySQL, and PostgreSQL are each used
- KPI definitions and computation logic
- Challenges encountered and how they were resolved
