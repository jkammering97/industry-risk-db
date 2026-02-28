#!/usr/bin/env bash
set -euo pipefail

COUNTRIES="${1:-040}"
PERIOD="${2:-2024}"
CMD_CODES="${3:-7208}"
FLOWS="${4:-M,X}"

# Load .env for dbt, which expects process-level env vars.
if [[ -f .env ]]; then
  eval "$(
    python3 - <<'PY'
from dotenv import dotenv_values
import shlex

for key, value in dotenv_values(".env").items():
    if value is None:
        continue
    print(f"export {key}={shlex.quote(value)}")
PY
  )"
fi

for required_var in AZURE_SQL_SERVER AZURE_SQL_DATABASE AZURE_SQL_USER AZURE_SQL_PASSWORD; do
  if [[ -z "${!required_var:-}" ]]; then
    echo "Missing required environment variable: ${required_var}"
    exit 1
  fi
done

echo "Applying SQL schema..."
python3 sql_bootstrap.py

IFS=',' read -r -a COUNTRY_CODES <<< "${COUNTRIES}"

if [[ ${#COUNTRY_CODES[@]} -eq 0 ]]; then
  echo "No country codes provided."
  exit 1
fi

echo "Running ingestion pipeline for countries: ${COUNTRIES}"
processed_count=0
for raw_country in "${COUNTRY_CODES[@]}"; do
  country_code="${raw_country//[[:space:]]/}"
  if [[ -z "${country_code}" ]]; then
    continue
  fi

  echo "  -> Country: ${country_code}"
  python3 risk_sql_pipeline.py \
    --country "${country_code}" \
    --period "${PERIOD}" \
    --cmd-codes "${CMD_CODES}" \
    --flows "${FLOWS}"
  processed_count=$((processed_count + 1))
done

if [[ ${processed_count} -eq 0 ]]; then
  echo "No valid country codes provided."
  exit 1
fi

echo "Running dbt models..."
(
  cd dbt_risk
  dbt run --profiles-dir profiles
)

echo "Running dbt tests..."
(
  cd dbt_risk
  dbt test --profiles-dir profiles
)

echo "Pipeline + dbt complete."
