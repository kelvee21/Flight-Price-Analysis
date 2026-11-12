import os
import sys

import pytest
from pyspark.sql import SparkSession

from include.config import STAGING_COLUMNS
from include.scripts.spark_transform import build_star_tables, clean, validate_staging


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("flight-price-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


def staging_row(airline="Biman", source="DAC", destination="CGP",
                base=100.0, tax=15.0, total=115.0, season="Eid"):
    row = dict.fromkeys(STAGING_COLUMNS)
    row.update(
        airline=airline, source=source, destination=destination,
        base_fare_bdt=base, tax_surcharge_bdt=tax, total_fare_bdt=total,
        seasonality=season, departure_datetime="2025-11-17 06:25:00",
    )
    return row


def make_df(spark, rows):
    schema = (
        "airline string, source string, source_name string, destination string, "
        "destination_name string, departure_datetime string, arrival_datetime string, "
        "duration_hrs double, stopovers string, aircraft_type string, class string, "
        "booking_source string, base_fare_bdt double, tax_surcharge_bdt double, "
        "total_fare_bdt double, seasonality string, days_before_departure int"
    )
    return spark.createDataFrame([[row[c] for c in STAGING_COLUMNS] for row in rows], schema)


def test_validate_staging_counts_problems(spark):
    df = make_df(spark, [
        staging_row(),
        staging_row(airline=" "),
        staging_row(source="??"),
        staging_row(base=-5.0, total=10.0),
        staging_row(tax=None),
        staging_row(total=999.0),
    ])

    report = validate_staging(df)

    assert report["input_rows"] == 6
    assert report["blank_airline_rows"] == 1
    assert report["invalid_airport_code_rows"] == 1
    assert report["negative_base_or_tax_rows"] == 1
    assert report["missing_base_or_tax_rows"] == 1
    assert report["total_fare_recomputed_rows"] == 1


def test_clean_rejects_bad_rows_and_recomputes_total(spark):
    df = make_df(spark, [
        staging_row(total=999.0),
        staging_row(airline=""),
        staging_row(source="??"),
        staging_row(base=-1.0),
        staging_row(tax=None),
    ])

    valid, rejected = clean(df)
    rows = valid.collect()

    assert len(rows) == 1
    assert float(rows[0].total_fare_bdt) == 115.0
    assert rows[0].departure_datetime is not None
    assert sorted(row.reject_reason for row in rejected.collect()) == [
        "blank airline",
        "invalid airport code",
        "missing base or tax fare",
        "negative base or tax fare",
    ]


def test_star_tables_keys_and_peak_flag(spark):
    df = make_df(spark, [
        staging_row(season="winter holidays"),
        staging_row(airline="US-Bangla", source="cgp", destination="dac", season="Regular"),
        staging_row(season=None),
    ])

    valid, _ = clean(df)
    tables = build_star_tables(valid)
    seasons = {row.seasonality: row.is_peak for row in tables["dim_season"].collect()}

    assert seasons == {"Winter Holidays": True, "Regular": False, "Unknown": False}
    assert tables["dim_airline"].count() == 2
    assert {(r.source, r.destination) for r in tables["dim_route"].collect()} == {("DAC", "CGP"), ("CGP", "DAC")}
    fact = tables["fact_flight_prices"]
    assert fact.count() == 3
    assert fact.filter("airline_key IS NULL OR route_key IS NULL OR season_key IS NULL").count() == 0
