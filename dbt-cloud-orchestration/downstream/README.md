# Downstream

Downstream processing assets for the Kaizen Wars project.

## Run

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration/downstream
uv sync
PYTHONPATH=/home/savas/dagster-test/dbt-cloud-orchestration:$PYTHONPATH dg dev --workspace workspace.yaml
```

Open http://localhost:3000 to see the downstream code location.

## Assets

- `fact_virtual_count_asset` - Counts records in stg_kaizen_wars__fact_virtual and writes to file

## Environment

Copy `.env` from the monorepo root or set these variables:

```bash
export DBT_CLOUD_TOKEN=...
export DBT_CLOUD_ACCOUNT_ID=...
export DBT_CLOUD_ACCESS_URL=...
export OUTPUT_DIR=...
```
