"""Shared settings for the flight-price pipeline."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CSV_PATH = Path(os.getenv(
    "FLIGHT_CSV_PATH",
    PROJECT_ROOT / "include" / "data" / "Flight_Price_Dataset_of_Bangladesh.csv",
))
WORK_DIR = Path(os.getenv("FLIGHT_WORK_DIR", PROJECT_ROOT / "include" / "data" / "processed"))

MYSQL_CONN_ID = os.getenv("MYSQL_CONN_ID", "mysql_staging")
PG_CONN_ID = os.getenv("PG_CONN_ID", "postgres_analytics")
STAGING_TABLE = "flight_staging.stg_flight_prices"
ANALYTICS_SCHEMA = "analytics"

# Columns that must exist in the source CSV.
REQUIRED_COLUMNS = [
    "Airline",
    "Source",
    "Destination",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
]

# Source CSV column -> staging/warehouse column.
COLUMN_MAP = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}
STAGING_COLUMNS = list(COLUMN_MAP.values())

# Seasonality labels counted as peak travel (the CSV uses "Winter Holidays").
PEAK_SEASONS = {"Eid", "Hajj", "Winter Holidays"}

# The run fails if more than this share of rows is rejected.
MAX_REJECTED_RATIO = float(os.getenv("MAX_REJECTED_RATIO", "0.05"))

# Failure alerts: a task that fails after all its retries sends an email through Gmail.
SMTP_CONN_ID = os.getenv("SMTP_CONN_ID", "smtp_gmail")
ALERT_FROM_EMAIL = os.getenv("GMAIL_USER", "")
# Comma-separated list. Empty means no alert emails are sent.
ALERT_EMAILS = [email.strip() for email in os.getenv("ALERT_EMAIL", "").split(",") if email.strip()]

# Retry settings shared by every task.
TASK_RETRIES = int(os.getenv("TASK_RETRIES", "2"))
TASK_RETRY_DELAY_MINUTES = int(os.getenv("TASK_RETRY_DELAY_MINUTES", "2"))
