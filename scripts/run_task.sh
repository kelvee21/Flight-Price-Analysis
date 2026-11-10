#!/usr/bin/env bash
# Run one task (native WSL setup): bash scripts/run_task.sh ingest_csv
set -euo pipefail
source ~/flight-price-venv/bin/activate
source "$(dirname "${BASH_SOURCE[0]}")/wsl_env.sh"
airflow tasks test flight_price_pipeline "${1:?Usage: run_task.sh <task_id>}"
