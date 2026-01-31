# dbt-cloud-orchestration

A Dagster + dbt Cloud orchestration monorepo with three independent code locations.

## Quick Start

### Run All Projects Together

From the monorepo root:

```bash
kc dgdev # Run dagster dev
kc test # Run tests
```

Open http://localhost:3000 in your browser.

### Run Individual Projects

Each sub-project can be developed independently:

```bash
# Ingestion team
cd ingestion/
kc dgdev # Run dagster dev
kc test # Run tests

# DBT team
cd dbt/
kc dgdev # Run dagster dev
kc test # Run tests

# Downstream team
cd downstream/
kc dgdev # Run dagster dev
kc test # Run tests
```

## Project Structure

```
dbt-cloud-orchestration/                       # Monorepo Root
├── workspace.yaml                             # Loads all 3 code locations
├── pyproject.toml                             # Root workspace configuration
├── uv.lock                                    # Shared workspace lockfile
├── data/                                      # Shared source data files
├── ingestion/                                 # Team 1: DLT pipeline project
│   ├── ingestion/                             # Python package
│   │   ├── definitions.py                     # Entry point
│   │   └── defs/                              # Supporting components
│   └── workspace.yaml
├── dbt/                                       # Team 2: dbt Cloud orchestration
│   ├── dbt_orchestration/                     # Python package (renamed to avoid collisions)
│   │   ├── definitions.py                     # Entry point
│   │   └── defs/                              # Supporting components
│   └── workspace.yaml
└── downstream/                                # Team 3: Downstream processing
    ├── downstream/                            # Python package
    │   ├── definitions.py                     # Entry point
    │   └── defs/                              # Supporting components
    └── workspace.yaml
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

# CSV Data directory (absolute path required for both DLT and Sling ingestion)
export CSV_DATA_PATH=/path/to/your/dbt-cloud-orchestration/data
```

## Data

Source data files are in the `data/` directory at the monorepo root:
- `raw_fact_virtual.csv` - Main fact data for the DLT pipeline
- `raw_customers.csv` - Customer dimension data
- `raw_orders.csv` - Orders dimension data
- `raw_payments.csv` - Payments dimension data

## Alias for Convenience

Add to your shell profile (`.bashrc` or `.zshrc`) while in the monorepo root:

```bash
export MONOREPO_ROOT=$(pwd)
alias dg-all='dg dev --workspace $MONOREPO_ROOT/workspace.yaml'
alias dg-ing='cd $MONOREPO_ROOT/ingestion && dg dev'
alias dg-dbt='cd $MONOREPO_ROOT/dbt && dg dev'
alias dg-down='cd $MONOREPO_ROOT/downstream && dg dev'
```
