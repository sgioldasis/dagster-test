# Ingestion

DLT Databricks pipeline for the Kaizen Wars project.

## Run

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration/ingestion
uv sync
dg dev --workspace workspace.yaml
```

Open http://localhost:3000 to see the ingestion code location.

## Assets

- `dlt_kaizen_wars_fact_virtual` - DLT pipeline that loads fact_virtual data to Databricks

## Environment

Copy `.env` from the monorepo root or set these variables:

```bash
export DATABRICKS_HOST=...
export DATABRICKS_TOKEN=...
export DATABRICKS_WAREHOUSE_ID=...
export DATABRICKS_CATALOG=...
export DATABRICKS_SCHEMA=...

# Data file path (defaults to monorepo root data folder)
export FACT_VIRTUAL_DATA_PATH=../data/raw_fact_virtual.csv
```

## Data

Data files are located in the monorepo root `data/` folder:
- `raw_fact_virtual.csv` - Source data for the DLT pipeline
