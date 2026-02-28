#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 6 ]]; then
  echo "Usage: $0 <resource-group> <location> <sql-server-name> <sql-db-name> <sql-admin-user> <sql-admin-password>"
  echo "Example: $0 rg-risk-observer westeurope risk-sql-001 industryriskdb sqladmin 'StrongPass!1234'"
  exit 1
fi

RESOURCE_GROUP="$1"
LOCATION="$2"
SQL_SERVER_NAME="$3"
SQL_DB_NAME="$4"
SQL_ADMIN_USER="$5"
SQL_ADMIN_PASSWORD="$6"

echo "Creating/validating resource group: ${RESOURCE_GROUP}"
az group create \
  --name "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --output none

echo "Deploying Azure SQL server + database..."
az deployment group create \
  --resource-group "${RESOURCE_GROUP}" \
  --template-file infra/sql.bicep \
  --parameters \
    location="${LOCATION}" \
    sqlServerName="${SQL_SERVER_NAME}" \
    sqlDatabaseName="${SQL_DB_NAME}" \
    sqlAdminLogin="${SQL_ADMIN_USER}" \
    sqlAdminPassword="${SQL_ADMIN_PASSWORD}" \
  --query properties.outputs \
  --output jsonc

echo
echo "Azure SQL deployed."
echo "Use these in .env:"
echo "AZURE_SQL_SERVER=${SQL_SERVER_NAME}.database.windows.net"
echo "AZURE_SQL_DATABASE=${SQL_DB_NAME}"
echo "AZURE_SQL_USER=${SQL_ADMIN_USER}"
echo "AZURE_SQL_PASSWORD=<your-password>"
