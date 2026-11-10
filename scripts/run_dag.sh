#!/usr/bin/env bash
# Run the whole DAG once (native WSL setup): bash scripts/run_dag.sh
set -euo pipefail
source ~/flight-price-venv/bin/activate
source "$(dirname "${BASH_SOURCE[0]}")/wsl_env.sh"
airflow dags test flight_price_pipeline
