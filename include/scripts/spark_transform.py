"""Spark data-quality checks and star-schema build."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from include.config import PEAK_SEASONS, STAGING_COLUMNS

AIRPORT_CODE = r"^[A-Za-z]{3}$"

FACT_COLUMNS = [
    "airline_key",
    "route_key",
    "season_key",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
    "departure_datetime",
    "arrival_datetime",
    "duration_hrs",
    "stopovers",
    "aircraft_type",
    "class",
    "booking_source",
    "days_before_departure",
]


def create_spark_session(app_name: str = "flight-price-analysis") -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def check_columns(df: DataFrame) -> None:
    missing = [column for column in STAGING_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing staging columns: {missing}")


def to_fare(column: str) -> Column:
    return F.expr(f"try_cast({column} AS DECIMAL(12, 2))")


def blank_to_null(column: str) -> Column:
    value = F.trim(F.col(column))
    return F.when(value == "", None).otherwise(value)


def count_if(condition: Column, name: str) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0)).alias(name)


def validate_staging(df: DataFrame) -> dict[str, int]:
    """Count data-quality problems in the staged rows (nothing is removed here)."""
    check_columns(df)
    base = to_fare("base_fare_bdt")
    tax = to_fare("tax_surcharge_bdt")
    total = to_fare("total_fare_bdt")

    row = df.agg(
        F.count(F.lit(1)).alias("input_rows"),
        count_if(blank_to_null("airline").isNull(), "blank_airline_rows"),
        count_if(
            blank_to_null("source").isNull() | blank_to_null("destination").isNull(),
            "blank_route_rows",
        ),
        count_if(
            ~F.trim("source").rlike(AIRPORT_CODE) | ~F.trim("destination").rlike(AIRPORT_CODE),
            "invalid_airport_code_rows",
        ),
        count_if(base.isNull() | tax.isNull(), "missing_base_or_tax_rows"),
        count_if((base < 0) | (tax < 0), "negative_base_or_tax_rows"),
        count_if(total.isNull() | (F.abs(total - (base + tax)) > 0.01), "total_fare_recomputed_rows"),
    ).first()
    return {name: int(value or 0) for name, value in row.asDict().items()}


def clean(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Standardise values and split rows into (valid, rejected)."""
    check_columns(df)
    df = (
        df.withColumn("airline", blank_to_null("airline"))
        .withColumn("source", F.upper(blank_to_null("source")))
        .withColumn("destination", F.upper(blank_to_null("destination")))
        .withColumn("seasonality", F.initcap(F.coalesce(blank_to_null("seasonality"), F.lit("Unknown"))))
        .withColumn("base_fare_bdt", to_fare("base_fare_bdt"))
        .withColumn("tax_surcharge_bdt", to_fare("tax_surcharge_bdt"))
    )
    base, tax = F.col("base_fare_bdt"), F.col("tax_surcharge_bdt")
    # The first failing rule wins; rows with no reason are valid.
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("airline").isNull(), "blank airline")
        .when(
            ~F.coalesce(F.col("source").rlike(AIRPORT_CODE) & F.col("destination").rlike(AIRPORT_CODE), F.lit(False)),
            "invalid airport code",
        )
        .when(base.isNull() | tax.isNull(), "missing base or tax fare")
        .when((base < 0) | (tax < 0), "negative base or tax fare"),
    )
    rejected = df.filter(F.col("reject_reason").isNotNull())
    valid = (
        df.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        .withColumn("total_fare_bdt", (F.col("base_fare_bdt") + F.col("tax_surcharge_bdt")).cast("decimal(12,2)"))
        .withColumn("departure_datetime", F.expr("try_to_timestamp(departure_datetime)"))
        .withColumn("arrival_datetime", F.expr("try_to_timestamp(arrival_datetime)"))
        .withColumn("duration_hrs", F.col("duration_hrs").cast("double"))
        .withColumn("days_before_departure", F.col("days_before_departure").cast("int"))
    )
    return valid, rejected


def build_star_tables(valid: DataFrame) -> dict[str, DataFrame]:
    """Build the three dimensions and the fact (grain: one valid source row)."""
    dim_airline = (
        valid.select("airline").distinct()
        .withColumn("airline_key", F.row_number().over(Window.orderBy("airline")))
        .select("airline_key", "airline")
    )
    dim_route = (
        valid.groupBy("source", "destination")
        .agg(F.min("source_name").alias("source_name"), F.min("destination_name").alias("destination_name"))
        .withColumn("route_key", F.row_number().over(Window.orderBy("source", "destination")))
        .select("route_key", "source", "source_name", "destination", "destination_name")
    )
    peak = [season.upper() for season in PEAK_SEASONS]
    dim_season = (
        valid.select("seasonality").distinct()
        .withColumn("season_key", F.row_number().over(Window.orderBy("seasonality")))
        .withColumn("is_peak", F.upper("seasonality").isin(peak))
        .select("season_key", "seasonality", "is_peak")
    )
    fact = (
        valid.join(dim_airline, "airline")
        .join(dim_route.select("route_key", "source", "destination"), ["source", "destination"])
        .join(dim_season.select("season_key", "seasonality"), "seasonality")
        .select(*FACT_COLUMNS)
    )
    return {
        "dim_airline": dim_airline,
        "dim_route": dim_route,
        "dim_season": dim_season,
        "fact_flight_prices": fact,
    }


def write_parquet(tables: dict[str, DataFrame], output_dir: Path) -> dict[str, str]:
    paths = {}
    for name, frame in tables.items():
        path = output_dir / name
        frame.write.mode("overwrite").parquet(str(path))
        paths[name] = str(path)
    return paths
