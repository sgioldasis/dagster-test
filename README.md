# Dagster Test Projects

A monorepo containing multiple Dagster-based data pipeline projects for experimenting with different integrations and patterns. This repository serves as a testbed for modern data engineering workflows using Dagster, dbt, Databricks, DLT, Sling, and more.

## Projects Overview

This repository contains the following independent projects:

| Project | Description | Python Version |
|---------|-------------|----------------|
| [`dbt-cloud-orchestration/`](dbt-cloud-orchestration/) | Multi-team dbt Cloud orchestration with ingestion, dbt Cloud integration, and downstream analytics | 3.10 - 3.14 |
| [`dbx-project/`](dbx-project/) | Dagster + Databricks integration with Sling ingestion and dbt transformations | 3.10 - 3.13 |
| [`dlt-project/`](dlt-project/) | Dagster + DLT (Data Load Tool) integration for data ingestion | 3.9 - 3.13 |
| [`dbt-cloud/`](dbt-cloud/) | Simple dbt Cloud integration example | - |
| [`sling-project/`](sling-project/) | Dagster + Sling integration for data replication | - |

## Prerequisites

### Required

- **[Devbox](https://www.jetify.com/devbox)** - Development environment management (includes Python, uv, PostgreSQL, and other tools)

### Optional (for specific projects)

- **Databricks account** - For Databricks-related features
- **dbt Cloud account** - For dbt Cloud orchestration features
- **PostgreSQL** - Managed automatically via Devbox

### Environment Variables

Some projects require environment variables. Add these to your `.bashrc` or `.zshrc` if needed:

```bash
# Databricks (for dbx-project and dbt-cloud-orchestration)
export DATABRICKS_TOKEN=<your_databricks_token>
export DATABRICKS_HOST=<your_databricks_host>

# SQL Server (for legacy connections)
export SQL_SERVER_PASSWORD=<your_sql_server_password>
```

## Quick Start

### 1. Enter Devbox Shell

```bash
devbox shell
```

This will:
- Activate the Python virtual environment
- Start PostgreSQL automatically
- Install required dependencies
- Set up the `kc` alias for task commands

### 2. Run a Project

Each project has its own setup. See the project-specific README files for details.

**Example - Run dbt-cloud-orchestration:**

```bash
# Navigate to the project
cd dbt-cloud-orchestration

# Copy environment variables
cp .env.example .env
# Edit .env with your credentials

# Run all code locations together
task dev -- .
```

## Task Commands (kc)

The repository includes a Taskfile with common commands:

```bash
# List all available tasks
kc

# Sync dependencies for a project
kc sync -- <project-folder>
# Example: kc sync -- dbt-cloud-orchestration

# Run Dagster dev server
kc dev -- <project-folder>
# Example: kc dev -- dbx-project

# Run tests
kc test -- <project-folder>
# Example: kc test -- dlt-project

# PostgreSQL helpers
kc pg-prepare    # Setup PostgreSQL and load jaffle_shop data
kc pg-cli        # Open PostgreSQL CLI

# DuckDB UI (for dbx-project)
kc ddb
```

## Project Structure

```
dagster-test/                          # Repository Root
├── .devbox/                           # Devbox configuration and data
├── dbt-cloud-orchestration/           # Multi-team dbt Cloud orchestration
│   ├── ingestion/                     # Team 1: Data ingestion (Sling/DLT)
│   ├── dbt/                           # Team 2: dbt Cloud integration
│   ├── downstream/                    # Team 3: Downstream analytics
│   └── data/                          # Source CSV files
├── dbx-project/                       # Databricks integration project
│   ├── src/dbx_project/               # Source code
│   ├── dbt/jdbt/                      # dbt project
│   └── data/                          # Sample data
├── dlt-project/                       # DLT integration project
│   ├── src/dlt_project/               # Source code
│   └── dbt/jdbt/                      # dbt project
├── dbt-cloud/                         # Simple dbt Cloud example
├── sling-project/                     # Sling integration example
├── devbox.json                        # Devbox configuration
├── Taskfile.yml                       # Common task definitions
└── requirements.txt                   # Global Python dependencies
```

## Database Connections

### PostgreSQL (Managed by Devbox)

PostgreSQL is automatically started when you enter the devbox shell.

```bash
# Create a database
createdb jaffle_shop

# Connect to database
psql -h localhost -d jaffle_shop

# Or use the task command
kc pg-cli
```

### Sling Connections

```bash
# Setup PostgreSQL connection
sling conns set POSTGRES_CONN url="postgresql://localhost:5432/jaffle_shop?sslmode=disable"

# Test connection
sling conns test POSTGRES_CONN

# List connections
sling conns list
```

## Project Details

### dbt-cloud-orchestration

The most complex project demonstrating multi-team collaboration:

- **Ingestion Team**: CSV → PostgreSQL → Databricks using Sling
- **dbt Team**: dbt Cloud integration for transformations
- **Downstream Team**: Analytics assets consuming dbt Cloud outputs

**Quick start:**
```bash
cd dbt-cloud-orchestration
cp .env.example .env
# Edit .env with your credentials
task dev -- .
```

See [`dbt-cloud-orchestration/README.md`](dbt-cloud-orchestration/README.md) for detailed setup.

### dbx-project

Databricks integration with:
- Sling-based data ingestion
- dbt transformations
- Databricks job orchestration

**Quick start:**
```bash
cd dbx-project
uv sync
dg dev
```

### dlt-project

Data ingestion using DLT (Data Load Tool):
- CSV to DuckDB ingestion
- dbt transformations
- Schedule-based automation

**Quick start:**
```bash
cd dlt-project
uv sync
dg dev
```

### sling-project

Standalone Sling integration:
- CSV to PostgreSQL replication
- SQL Server integration examples

**Quick start:**
```bash
cd sling-project
uv sync
kc pg-prepare  # Load sample data
dg dev
```

## Environment Variables Reference

### dbt-cloud-orchestration

See [`dbt-cloud-orchestration/.env.example`](dbt-cloud-orchestration/.env.example) for the complete list.

**Key variables:**

| Variable | Description | Required For |
|----------|-------------|--------------|
| `DBT_CLOUD_TOKEN` | dbt Cloud API token | dbt code location |
| `DBT_CLOUD_ACCOUNT_ID` | Your dbt Cloud account ID | dbt code location |
| `DBT_CLOUD_PROJECT_ID` | dbt Cloud project ID | dbt code location |
| `DBT_CLOUD_ENVIRONMENT_ID` | dbt Cloud environment ID | dbt code location |
| `DATABRICKS_HOST` | Databricks workspace URL | Databricks features |
| `DATABRICKS_TOKEN` | Databricks personal access token | Databricks features |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID | Databricks features |
| `CSV_DATA_PATH` | Absolute path to data directory | Sling ingestion |

### dbx-project

Uses `DATABRICKS_TOKEN` and `DATABRICKS_HOST` from environment or `.env` file.

## Development Workflow

### Adding a New Project

1. Create a new folder: `mkdir my-new-project`
2. Initialize with `uvx create-dagster@latest project my-new-project`
3. Add a `pyproject.toml` with dependencies
4. Create a `README.md` with setup instructions
5. Update the main `README.md` (this file) to include the new project

### Running Tests

```bash
# For a specific project
kc test -- <project-folder>

# Or manually
cd <project-folder>
uv run pytest -v
```

### Code Style

This repository uses:
- **ruff** for linting and formatting
- **pyright** for type checking (in some projects)

```bash
# Run linting
ruff check .

# Run formatting
ruff format .
```

## Troubleshooting

### PostgreSQL Issues

```bash
# Check PostgreSQL status
pg_ctl status -D .devbox/var/lib/postgres

# Restart PostgreSQL
pg_ctl restart -D .devbox/var/lib/postgres -l .devbox/var/log/postgres.log
```

### Port Already in Use

Dagster will automatically try the next available port (3001, 3002, etc.). Check console output for the actual URL.

### UV Sync Issues

```bash
# Clear uv cache
uv cache clean

# Re-sync
uv sync
```

## Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Sling Documentation](https://docs.slingdata.io/)
- [DLT Documentation](https://dlthub.com/docs/intro)
- [dbt Cloud API Documentation](https://docs.getdbt.com/dbt-cloud/api-v2)
- [Databricks Documentation](https://docs.databricks.com/)

## Authoring Examples

See [`AGENTS.md`](AGENTS.md) for detailed coding patterns and examples used across projects.

## License

This is a test/learning repository. See individual project dependencies for their respective licenses.
