# dbt

dbt Cloud assets for the Kaizen Wars project.

## Run

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration/dbt
uv sync
PYTHONPATH=/home/savas/dagster-test/dbt-cloud-orchestration:$PYTHONPATH dg dev --workspace workspace.yaml
```

Open http://localhost:3000 to see the dbt code location.

## Assets

- `stg_kaizen_wars__fact_virtual` - Staging model from dbt Cloud
- `my_dbt_cloud_assets` - All dbt Cloud assets

## Environment

Copy `.env` from the monorepo root or set these variables:

```bash
export DBT_CLOUD_TOKEN=...
export DBT_CLOUD_ACCOUNT_ID=...
export DBT_CLOUD_ACCESS_URL=...
export DBT_CLOUD_PROJECT_ID=...
export DBT_CLOUD_ENVIRONMENT_ID=...
export DBT_CLOUD_JOB_ID=...
```
