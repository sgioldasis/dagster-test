# dbt-cloud-orchestration

A Dagster + dbt Cloud orchestration monorepo demonstrating multi-team data pipeline collaboration with three independent code locations.

## Overview

This project implements an ELT (Extract, Load, Transform) pipeline that showcases:

- **Data Ingestion**: CSV → PostgreSQL → Databricks using [Sling](https://slingdata.io/)
- **Data Transformation**: dbt Cloud for managed data transformations
- **Downstream Analytics**: Post-processing assets that consume dbt Cloud outputs

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION CODE LOCATION                         │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌─────────────┐ │
│  │ CSV Files   │───▶│csv_fact_     │───▶│fact_virtual  │───▶│Databricks   │ │
│  │ (data/)     │    │virtual       │    │              │    │Job (opt)    │ │
│  └─────────────┘    │(Sling)       │    │(Sling)       │    │             │ │
│                     └──────────────┘    └──────────────┘    └─────────────┘ │
│                           │                      │                           │
│                     PostgreSQL            Databricks                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                DBT CODE LOCATION                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    dbt Cloud Workspace                                   │ │
│  │  ┌─────────────────┐    ┌─────────────────────────────────────────────┐ │ │
│  │  │ fact_virtual    │───▶│ stg_kaizen_wars__fact_virtual               │ │ │
│  │  │ (Source Asset)  │    │ (dbt Cloud Model)                           │ │ │
│  │  └─────────────────┘    └─────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DOWNSTREAM CODE LOCATION                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  ┌─────────────────────────────┐    ┌─────────────────────────────────┐  │ │
│  │  │ dbt_optimove/               │───▶│ downstream/                     │  │ │
│  │  │ stg_kaizen_wars__fact_virtual│   │ fact_virtual_count              │  │ │
│  │  └─────────────────────────────┘    │ (JSON output: output/)          │  │ │
│  │                                     └─────────────────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Python 3.10 - 3.14
- [uv](https://docs.astral.sh/uv/) for dependency management
- (Optional) Databricks workspace for Databricks features
- (Optional) dbt Cloud account for dbt Cloud features

## Quick Start

### 1. Configure Environment Variables

Copy the example environment file and fill in your values:

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required variables depend on which features you want to use:

| Feature | Required Variables |
|---------|-------------------|
| Basic (local) | `CSV_DATA_PATH` (absolute path to `data/` directory) |
| PostgreSQL ingestion | `LOCAL_POSTGRES_HOST`, `LOCAL_POSTGRES_PORT`, `LOCAL_POSTGRES_USER`, `LOCAL_POSTGRES_PASSWORD` |
| Databricks | `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID` |
| dbt Cloud | `DBT_CLOUD_TOKEN`, `DBT_CLOUD_ACCOUNT_ID`, `DBT_CLOUD_PROJECT_ID`, `DBT_CLOUD_ENVIRONMENT_ID` |

### 2. Run All Projects Together

```bash
kc dev -- dbt-cloud-orchestration
```

Or directly:

```bash
cd dbt-cloud-orchestration && kc dev
```

Open http://localhost:3000 in your browser.

### 3. Run Tests

```bash
kc test
```

Or directly:

```bash
cd dbt-cloud-orchestration && kc test
```

## Development by Team

Each sub-project can be developed independently:

### Ingestion Team

```bash
cd ingestion/

# Run Dagster development server
kc dev

# Run tests
kc test
```

**Key Assets:**
- `csv_fact_virtual` - Loads CSV data into PostgreSQL (Sling)
- `fact_virtual` - Replicates data from PostgreSQL to Databricks (Sling)
- `run_databricks_ingestion_job` - Triggers Databricks notebook job (optional)

**Schedule:** Runs every 2 minutes (`csv_fact_virtual_schedule`)

### dbt Team

```bash
cd dbt/

# Run Dagster development server
kc dev

# Run tests
kc test
```

**Key Assets:**
- `fact_virtual` - Source asset from ingestion code location
- `stg_kaizen_wars__fact_virtual` - Staging model from dbt Cloud

**Automation:** Assets trigger automatically when upstream dependencies update (`AutomationCondition.any_deps_updated()`)

### Downstream Team

```bash
cd downstream/

# Run Dagster development server
kc dev

# Run tests
kc test
```

**Key Assets:**
- `downstream/fact_virtual_count` - Consumes dbt Cloud output and writes JSON with record count

**Output:** `output/fact_virtual_count.json`

## Project Structure

```
dbt-cloud-orchestration/                    # Monorepo Root
├── workspace.yaml                          # Loads all 3 code locations
├── pyproject.toml                          # Root workspace configuration
├── uv.lock                                 # Shared workspace lockfile
├── .env.example                            # Example environment variables
├── data/                                   # Shared source data files
│   ├── raw_fact_virtual.csv                # Main fact data
│   ├── raw_customers.csv                   # Customer dimension
│   ├── raw_orders.csv                      # Orders dimension
│   └── raw_payments.csv                    # Payments dimension
├── ingestion/                              # Team 1: Data ingestion
│   ├── src/ingestion/
│   │   ├── definitions.py                  # Main entry point
│   │   ├── components/                     # Dagster components
│   │   │   └── ingestion_sling/            # Sling replication configs
│   │   └── defs/                           # Resources and utilities
│   ├── pyproject.toml
│   └── workspace.yaml
├── dbt/                                    # Team 2: dbt Cloud orchestration
│   ├── src/dbt_orchestration/
│   │   ├── definitions.py                  # Main entry point
│   │   └── defs/                           # Assets, resources, types
│   ├── pyproject.toml
│   └── workspace.yaml
└── downstream/                             # Team 3: Downstream processing
    ├── src/downstream/
    │   ├── definitions.py                  # Main entry point
    │   └── defs/
    │       └── fact_virtual_count.py       # Asset implementation
    ├── pyproject.toml
    └── workspace.yaml
```

## Environment Variables

See [`.env.example`](.env.example) for the complete list. Key variables:

### dbt Cloud Configuration

| Variable | Description | Required For |
|----------|-------------|--------------|
| `DBT_CLOUD_TOKEN` | dbt Cloud API token | dbt code location |
| `DBT_CLOUD_ACCOUNT_ID` | Your dbt Cloud account ID | dbt code location |
| `DBT_CLOUD_ACCESS_URL` | dbt Cloud URL (default: `https://cloud.getdbt.com`) | dbt code location |
| `DBT_CLOUD_PROJECT_ID` | dbt Cloud project ID | dbt code location |
| `DBT_CLOUD_ENVIRONMENT_ID` | dbt Cloud environment ID | dbt code location |
| `DBT_CLOUD_JOB_ID` | Specific job ID to trigger (optional) | Manual job triggers |
| `DBT_CLOUD_RUN_TIMEOUT_SECONDS` | Timeout for dbt Cloud runs (default: 600) | dbt code location |
| `DBT_CLOUD_POLLING_INTERVAL` | Polling interval in seconds (default: 30) | dbt code location |

### Databricks Configuration (Ingestion)

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Databricks workspace URL (e.g., `adb-xxx.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID |
| `DATABRICKS_CATALOG` | Catalog name (default: `test`) |
| `DATABRICKS_SCHEMA` | Schema name (default: `main`) |
| `DATABRICKS_NOTEBOOK_JOB_ID` | Job ID for optional notebook trigger |

### PostgreSQL Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `LOCAL_POSTGRES_HOST` | PostgreSQL host | `localhost` |
| `LOCAL_POSTGRES_PORT` | PostgreSQL port | `5432` |
| `LOCAL_POSTGRES_USER` | PostgreSQL username | - |
| `LOCAL_POSTGRES_PASSWORD` | PostgreSQL password | - |
| `LOCAL_POSTGRES_DATABASE` | Database name | `postgres` |

### Data Paths

| Variable | Description |
|----------|-------------|
| `CSV_DATA_PATH` | **Absolute** path to the `data/` directory (required for Sling ingestion) |
| `OUTPUT_DIR` | Output directory for downstream assets (default: `./output`) |

## Data Flow

1. **Ingestion** (`ingestion/` code location)
   - `csv_fact_virtual` schedule triggers every 2 minutes
   - Sling loads `data/raw_fact_virtual.csv` → PostgreSQL
   - Sling replicates PostgreSQL → Databricks
   - Optional: Databricks notebook job is triggered

2. **Transformation** (`dbt/` code location)
   - dbt Cloud models reference the `fact_virtual` source asset
   - `stg_kaizen_wars__fact_virtual` transforms the ingested data
   - Automation sensor triggers on upstream updates

3. **Analytics** (`downstream/` code location)
   - `fact_virtual_count` asset depends on dbt Cloud output
   - Queries dbt Cloud API for run information
   - Writes record count to `output/fact_virtual_count.json`

## Automation & Scheduling

| Component | Type | Trigger |
|-----------|------|---------|
| `csv_fact_virtual_schedule` | Schedule | Every 2 minutes |
| `default_automation_sensor` | Sensor | Asset dependencies updated |
| `freshness_sensor` | Sensor | Data freshness checks |
| `dbt_cloud_polling_sensor` | Sensor | dbt Cloud run status |


## Troubleshooting

### CSV files not found

`CSV_DATA_PATH` must be an **absolute** path:

```bash
# Wrong
export CSV_DATA_PATH=./data

# Correct
export CSV_DATA_PATH=/home/user/projects/dbt-cloud-orchestration/data
```

### dbt Cloud assets not loading

Verify your environment variables:

```bash
# Check if variables are set
echo $DBT_CLOUD_TOKEN
echo $DBT_CLOUD_ACCOUNT_ID

# Test dbt Cloud API access
curl -H "Authorization: Bearer $DBT_CLOUD_TOKEN" \
  "$DBT_CLOUD_ACCESS_URL/api/v2/accounts/$DBT_CLOUD_ACCOUNT_ID/projects/"
```

### Port already in use

If port 3000 is already in use, Dagster will automatically try the next available port (3001, 3002, etc.). Check the console output for the actual URL.

### Database connection errors

Ensure PostgreSQL is running and accessible:

```bash
# Test PostgreSQL connection
psql -h $LOCAL_POSTGRES_HOST -p $LOCAL_POSTGRES_PORT -U $LOCAL_POSTGRES_USER -d $LOCAL_POSTGRES_DATABASE -c "SELECT 1"
```

## Further Documentation

- [Ingestion README](ingestion/README.md) - Detailed Sling configuration guide
- [dbt README](dbt/README.md) - dbt Cloud setup instructions
- [Downstream README](downstream/README.md) - Downstream asset documentation
- [SUMMARY.md](SUMMARY.md) - Project summary and architecture notes

## References

- [Dagster Documentation](https://docs.dagster.io/)
- [Sling Documentation](https://docs.slingdata.io/)
- [dbt Cloud API Documentation](https://docs.getdbt.com/dbt-cloud/api-v2)
