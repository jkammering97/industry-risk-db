# Azure Setup (Storage + Layered Risk DB + Streamlit)

## 1. Prerequisites

- Azure CLI installed and logged in:
  - `az login`
- Python dependencies installed:
  - `pip install -r requirements.txt`

## 2. Provision storage account + risk tables

Run:

```bash
./scripts/deploy_azure_storage.sh <resource-group> <location> <storage-account-name>
```

Example:

```bash
./scripts/deploy_azure_storage.sh rg-risk-observer westeurope riskobserver12345
```

This deploys:
- Storage account
- Table storage "DB" layers:
  - `hhirisk`
  - `logisticsrisk`
  - `policyrisk`

It also prints an `AZURE_STORAGE_CONNECTION_STRING` value.

## 3. Configure environment

Add to `.env`:

```env
AZURE_STORAGE_CONNECTION_STRING=<your-connection-string>
```

## 4. Seed initial records

```bash
python3 seed_risk_layers.py
```

This writes sample data for Austria (`country_code=040`) into all three layers.

## 5. Run the layered dashboard

```bash
streamlit run risk_dashboard_layers.py
```

## Data model notes

- Partition key: `country_code` (example `040` for Austria)
- Three logical layers:
  - HHI concentration risk
  - Logistics disruption risk
  - Policy/tariff/sanctions risk
- Combined supplier risk is calculated as:
  - `0.5 * HHI + 0.3 * Logistics + 0.2 * Policy`

Adjust those weights in `risk_layers_store.py` when needed.
