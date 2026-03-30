# Azure SQL + dbt Setup (for Risk Layers)

## setup

- **Storage Account** for optional raw files / backups (bronze)
- **Azure SQL Database** for structured raw/staging/mart tables
- **dbt** for repeatable transformations into risk layers
- **Streamlit** reading from `mart.*` tables

## Important note on dbt Python models

For **Azure SQL / dbt-sqlserver**, use dbt SQL models for transformations.
Python execution in dbt is adapter-dependent and not the reliable path here.
This repo therefore implements:

- Python ingestion job (`risk_sql_pipeline.py`) for fetching/loading raw data
- dbt SQL models (`dbt_risk/models/*`) for risk-layer transformation

## 1. Deploy Azure SQL

```bash
./scripts/deploy_azure_sql.sh \
  <resource-group> \
  <location> \
  <sql-server-name> \
  <sql-db-name> \
  <sql-admin-user> \
  <sql-admin-password>
```

## 2. Configure environment variables

Add to `.env`:

```env
AZURE_SQL_SERVER=<server>.database.windows.net
AZURE_SQL_DATABASE=<db-name>
AZURE_SQL_USER=<admin-user>
AZURE_SQL_PASSWORD=<admin-password>
AZURE_SQL_DRIVER=ODBC Driver 17 for SQL Server
```

Comtrade key is still needed:

```env
comtrade_subscription_key=<your-key>
```

## 3. Install dependencies

```bash
pip install -r requirements.txt
pip install -r requirements-sql.txt
pip install -r dbt_risk/requirements-dbt.txt
```

## 4. Create SQL schemas and tables

```bash
python3 sql_bootstrap.py
```

Creates:

- `raw.comtrade_trade`
- `raw.logistics_signals`
- `raw.policy_signals`

and schemas: `raw`, `staging`, `mart`.

## 5. Run ingestion + dbt

```bash
./scripts/run_pipeline_and_dbt.sh 040 2024 7208 M,X
```

This will:

1. Ensure schema exists
2. Fetch and write raw data
3. Build dbt models:
   - `mart.hhi_layer`
   - `mart.logistics_layer`
   - `mart.policy_layer`
   - `mart.supplier_risk`

## 6. Run SQL dashboard

```bash
streamlit run risk_dashboard_sql.py
```

## Table design (ready for recurring updates)

The schema is layered for incremental refresh:

1. **raw**: append-only ingestions with `ingest_id` and `ingested_at`
2. **staging**: typed and standardized columns
3. **mart**: business-ready risk layers + blended supplier risk

This supports:

- Rebuildable marts
- Backfills by `period` / `ingest_id`
- Auditability of historical runs

## Recurring updates (recommended initial cadence)

Start with **daily at 06:00 Europe/Vienna**.

### Option A (recommended): Azure Container Apps Job

- Container command: `./scripts/run_pipeline_and_dbt.sh 040 2024 7208,7210,7212 M,X`
- Schedule daily
- Store secrets in managed environment secrets
- Log to Log Analytics

### Option B: Azure Functions Timer Trigger

- Timer trigger invokes shell/Python entrypoint
- Good for low-cost, low-frequency runs

### Option C: Azure Data Factory

- More orchestration-heavy but good if you add many external feeds

## Next schema improvements

When you replace heuristics with external datasets, keep stable columns and add:

- `source_name`
- `source_version`
- `effective_from`
- `effective_to`
- `confidence_score`

This allows signal provenance and model governance from day one.
