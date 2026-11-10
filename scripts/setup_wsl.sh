#!/usr/bin/env bash
# Native (no-Docker) setup inside WSL2 (Kali/Ubuntu).
# Run: bash scripts/setup_wsl.sh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
else
  echo "Missing .env. Copy .env.example to .env and set POSTGRES_PASSWORD first."
  exit 1
fi

echo "==> Updating apt"
sudo apt-get update -y

echo "==> Installing MySQL server + client"
sudo apt-get install -y mysql-server mysql-client
echo "==> Installing Java 21 for PySpark"
sudo apt-get install -y openjdk-21-jre-headless

echo "==> Ensuring PostgreSQL is available (already installed on Kali)"
sudo service postgresql start

echo "==> Configuring shared PostgreSQL database and user"
sudo -u postgres psql \
  -v postgres_user="$POSTGRES_USER" \
  -v postgres_password="$POSTGRES_PASSWORD" \
  -v postgres_db="$POSTGRES_DB" \
  -f include/sql/setup_airflow_pg.sql

echo "==> Configuring MySQL staging (create db + user)"
sudo service mysql start
sudo mysql -e "CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE} CHARACTER SET utf8mb4;"
sudo mysql -e "CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'localhost' IDENTIFIED BY '${MYSQL_PASSWORD}';"
sudo mysql -e "GRANT ALL PRIVILEGES ON ${MYSQL_DATABASE}.* TO '${MYSQL_USER}'@'localhost'; FLUSH PRIVILEGES;"

echo "==> Creating Python 3.13 virtualenv"
python3.13 -m venv ~/flight-price-venv
source ~/flight-price-venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "==> Configuring Airflow to use the shared PostgreSQL database"
source scripts/wsl_env.sh
mkdir -p "$AIRFLOW_HOME"

echo "==> Initialising Airflow metadata DB"
airflow db migrate

echo "==> Writing the UI login (Airflow 3 simple auth manager)"
echo "{\"${_AIRFLOW_WWW_USER_USERNAME:-admin}\": \"${_AIRFLOW_WWW_USER_PASSWORD:-admin}\"}" \
  > "$AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"

echo "==> Registering Airflow connections"
bash scripts/setup_connections.sh

echo
echo "Done. Start Airflow with:  source ~/flight-price-venv/bin/activate && source scripts/wsl_env.sh && airflow standalone"
echo "UI: http://localhost:8080  (admin / admin)"