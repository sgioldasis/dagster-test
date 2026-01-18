# dbt-cloud-orchestration

A Dagster + dbt Cloud orchestration monorepo with three independent code locations.

## Quick Start

### Run All Projects Together

From the monorepo root:

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration
uv sync --all-packages
PYTHONPATH=/home/savas/dagster-test/dbt-cloud-orchestration:$PYTHONPATH dg dev --workspace workspace.yaml
```

Open http://localhost:3000 in your browser.

### Run Individual Projects

Each sub-project can be developed independently:

```bash
# Ingestion team
cd /home/savas/dagster-test/dbt-cloud-orchestration/ingestion
uv sync
PYTHONPATH=/home/savas/dagster-test/dbt-cloud-orchestration:$PYTHONPATH dg dev --workspace workspace.yaml

# DBT team
cd /home/savas/dagster-test/dbt-cloud-orchestration/dbt
uv sync
PYTHONPATH=/home/savas/dagster-test/dbt-cloud-orchestration:$PYTHONPATH dg dev --workspace workspace.yaml

# Downstream team
cd /home/savas/dagster-test/dbt-cloud-orchestration/downstream
uv sync
PYTHONPATH=/home/savas/dagster-test/dbt-cloud-orchestration:$PYTHONPATH dg dev --workspace workspace.yaml
```

## Project Structure

```
dbt-cloud-orchestration/
├── workspace.yaml              # Loads all 3 code locations
├── pyproject.toml              # Root workspace (uv workspace)
├── uv.lock                     # Shared lockfile
├── data/                       # Source data files
│   ├── raw_fact_virtual.csv
│   ├── raw_customers.csv
│   ├── raw_orders.csv
│   └── raw_payments.csv
├── ingestion/                  # Team 1: DLT pipeline
│   ├── pyproject.toml
│   ├── src/
│   │   └── defs/
│   │       └── definitions.py
│   └── .env                    # Environment variables
├── dbt/                        # Team 2: dbt Cloud
│   ├── pyproject.toml
│   └── src/
│       └── defs/
│           └── definitions.py
└── downstream/                 # Team 3: Downstream
    ├── pyproject.toml
    └── src/
        └── defs/
            └── definitions.py
```

## Code Locations

| Location | Purpose | Key Asset |
|----------|---------|-----------|
| `ingestion` | DLT Databricks pipelines | `dlt_kaizen_wars_fact_virtual` |
| `dbt` | dbt Cloud assets | `stg_kaizen_wars__fact_virtual` |
| `downstream` | Post-processing | `fact_virtual_count_asset` |

## Environment Variables

Copy `.env` to each subfolder or set these variables:

```bash
# dbt Cloud
export DBT_CLOUD_TOKEN=...
export DBT_CLOUD_ACCOUNT_ID=...
export DBT_CLOUD_ACCESS_URL=...
export DBT_CLOUD_PROJECT_ID=...
export DBT_CLOUD_ENVIRONMENT_ID=...

# Databricks (for ingestion)
export DATABRICKS_HOST=...
export DATABRICKS_TOKEN=...
export DATABRICKS_WAREHOUSE_ID=...

# Data files (relative to sub-project directory)
export FACT_VIRTUAL_DATA_PATH=../data/raw_fact_virtual.csv
```

## Data

Source data files are in the `data/` directory at the monorepo root:
- `raw_fact_virtual.csv` - Main fact data for the DLT pipeline
- `raw_customers.csv` - Customer dimension data
- `raw_orders.csv` - Orders dimension data
- `raw_payments.csv` - Payments dimension data

## Alias for Convenience

Add to your shell profile (`.bashrc` or `.zshrc`):

```bash
export MONOREPO_ROOT=/home/savas/dagster-test/dbt-cloud-orchestration
alias dg-all='PYTHONPATH=$MONOREPO_ROOT:$PYTHONPATH dg dev --workspace $MONOREPO_ROOT/workspace.yaml'
alias dg-ing='cd $MONOREPO_ROOT/ingestion && PYTHONPATH=$MONOREPO_ROOT:$PYTHONPATH dg dev --workspace workspace.yaml'
alias dg-dbt='cd $MONOREPO_ROOT/dbt && PYTHONPATH=$MONOREPO_ROOT:$PYTHONPATH dg dev --workspace workspace.yaml'
alias dg-down='cd $MONOREPO_ROOT/downstream && PYTHONPATH=$MONOREPO_ROOT:$PYTHONPATH dg dev --workspace workspace.yaml'
```
