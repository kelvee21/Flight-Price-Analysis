#!/usr/bin/env bash
# Source this file before running Airflow in WSL2: source scripts/wsl_env.sh
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  . "$PROJECT_ROOT/.env"
  set +a
fi

export AIRFLOW_HOME="$HOME/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env}@localhost:${PG_CONN_PORT:-5432}/${POSTGRES_DB:-airflow}"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__CORE__LOAD_EXAMPLES="false"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="false"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${_AIRFLOW_WWW_USER_USERNAME:-admin}:admin"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="$AIRFLOW_HOME/passwords.json"
export AIRFLOW_CONN_MYSQL_STAGING="mysql://${MYSQL_USER:-airflow}:${MYSQL_PASSWORD:-airflow}@127.0.0.1:${MYSQL_CONN_PORT:-3306}/${MYSQL_DATABASE:-flight_staging}"
export AIRFLOW_CONN_POSTGRES_ANALYTICS="postgresql://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD}@localhost:${PG_CONN_PORT:-5432}/${POSTGRES_DB:-airflow}"
export FLIGHT_CSV_PATH="$PROJECT_ROOT/include/data/Flight_Price_Dataset_of_Bangladesh.csv"
export FLIGHT_WORK_DIR="$PROJECT_ROOT/include/data/processed"
export SPARK_MASTER="${SPARK_MASTER:-local[*]}"
export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
