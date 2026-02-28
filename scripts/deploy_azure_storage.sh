#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <resource-group> <location> <storage-account-name>"
  echo "Example: $0 rg-risk-observer westeurope riskobs$(date +%s)"
  exit 1
fi

RESOURCE_GROUP="$1"
LOCATION="$2"
STORAGE_ACCOUNT_NAME="$3"

echo "Creating/validating resource group: ${RESOURCE_GROUP}"
az group create \
  --name "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --output none

echo "Deploying storage account + risk tables with Bicep..."
az deployment group create \
  --resource-group "${RESOURCE_GROUP}" \
  --template-file infra/main.bicep \
  --parameters storageAccountName="${STORAGE_ACCOUNT_NAME}" location="${LOCATION}" \
  --query properties.outputs \
  --output jsonc

CONNECTION_STRING="$(az storage account show-connection-string \
  --resource-group "${RESOURCE_GROUP}" \
  --name "${STORAGE_ACCOUNT_NAME}" \
  --query connectionString \
  --output tsv)"

echo
echo "Provisioning done."
echo "Add this to your .env:"
echo "AZURE_STORAGE_CONNECTION_STRING='${CONNECTION_STRING}'"
