#!/usr/bin/env bash
# Register Airflow connections used by the pipeline.
set -euo pipefail

airflow connections delete mysql_staging 2>/dev/null || true
airflow connections delete postgres_analytics 2>/dev/null || true

airflow connections add mysql_staging \
  --conn-type mysql \
  --conn-host "${MYSQL_CONN_HOST:-mariadb}" \
  --conn-port "${MYSQL_CONN_PORT:-3306}" \
  --conn-login "${MYSQL_USER:-airflow}" \
  --conn-password "${MYSQL_PASSWORD:-airflow}" \
  --conn-schema "${MYSQL_DATABASE:-flight_staging}"

airflow connections add postgres_analytics \
  --conn-type postgres \
  --conn-host "${PG_CONN_HOST:-postgres}" \
  --conn-port "${PG_CONN_PORT:-5432}" \
  --conn-login "${POSTGRES_USER:-airflow}" \
  --conn-password "${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env}" \
  --conn-schema "${POSTGRES_DB:-airflow}"

echo "Connections registered."