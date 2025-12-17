# Environment Configuration Guide

This document explains how to configure and use the environment-based setup for the dbx-project.

## Overview

The project supports two environments:
- **LOCAL**: Uses DuckDB for local development
- **PROD**: Uses Databricks for production

## Environment Configuration

### Setting the Environment

The environment is controlled by the `DAGSTER_ENV` variable in the `.env` file at the project root. The system uses Dagster's [`EnvVar`](src/dbx_project/definitions.py:6) to read this variable at runtime.

#### LOCAL Environment (Default)
```bash
DAGSTER_ENV=LOCAL
```
- Uses DuckDB as the target database
- Database file: `${LOCAL_DUCKDB_PATH}`
- File location: [`${LOCAL_DUCKDB_PATH}`](${LOCAL_DUCKDB_PATH})
- **Configuration Location**: [`profiles.yml`](dbt/jdbt/profiles.yml:3) local target `path` parameter
- **Sling Connection**: Sling reads the DuckDB connection from the [`local`](dbt/jdbt/profiles.yml:3) target in [`profiles.yml`](dbt/jdbt/profiles.yml:3)
- **Sling Replication File**: [`replication_local.yaml`](src/dbx_project/defs/ingest_files/replication_local.yaml:1)
- **Template Variable**: `env` (used in YAML templates)
- Ideal for development and testing

#### PROD Environment
```bash
DAGSTER_ENV=PROD
```
- Uses Databricks as the target database
- Host: `dbc-a8f6bc8e-f728.cloud.databricks.com`
- Schema: `main`
- Requires `DATABRICKS_TOKEN` environment variable

## Configuration Files

### Replication Configuration

The system automatically loads the appropriate replication configuration based on the environment:

- **LOCAL**: `src/dbx_project/defs/ingest_files/replication_local.yaml`
- **PROD**: `src/dbx_project/defs/ingest_files/replication_prod.yaml`

Both configurations use the same data sources:
- `file://data/raw_customers.csv` → `main.raw_customers`
- `file://data/raw_orders.csv` → `main.raw_orders`
- `file://data/raw_payments.csv` → `main.raw_payments`

### DBT Profiles

The DBT profiles are dynamically selected based on the environment:

- **LOCAL**: Uses DuckDB profile with file-based storage
- **PROD**: Uses Databricks profile with existing configuration

## Usage

### Switching Environments

1. Edit the `.env` file:
   ```bash
   # For local development
   DAGSTER_ENV=LOCAL
   
   # For production
   DAGSTER_ENV=PROD
   ```

2. The system will automatically:
   - Load the correct replication configuration
   - Use the appropriate DBT profile
   - Connect to the correct database target

### Running the Project

#### Local Development
```bash
# Ensure DAGSTER_ENV=LOCAL in .env file
dagster dev
```

#### Production
```bash
# Ensure DAGSTER_ENV=PROD in .env file
# Set DATABRICKS_TOKEN environment variable
export DATABRICKS_TOKEN="your_token_here"
dagster dev
```

## Dependencies

The project includes `python-dotenv` for environment variable management. This is automatically loaded when the project starts.

## Sling DUCKDB Connection Configuration

When using LOCAL environment (`DAGSTER_ENV=LOCAL`), Sling uses the following configuration to connect to DuckDB:

### Sling Replication File
- **Configuration File**: [`replication_local.yaml`](src/dbx_project/defs/ingest_files/replication_local.yaml:1)
- **Target Definition**: `target: DUCKDB`

### Connection Parameters (from profiles.yml)
Sling reads the DuckDB connection parameters from the [`local`](dbt/jdbt/profiles.yml:3) target in [`profiles.yml`](dbt/jdbt/profiles.yml:3):

```yaml
local:
  type: duckdb
  path: /tmp/jaffle_platform.duckdb  # ← Sling uses this path
  threads: 1
```

### How Sling Knows Which DuckDB Database File to Use

1. **Profile Selection**: When `DAGSTER_ENV=LOCAL`, the system selects the [`local`](dbt/jdbt/profiles.yml:3) target from [`profiles.yml`](dbt/jdbt/profiles.yml:3)
2. **Connection Parameters**: Sling reads the connection parameters from this profile:
   - `type: duckdb` - Specifies the database type
   - `path: /tmp/jaffle_platform.duckdb` - Specifies the database file location
   - `threads: 1` - Specifies the number of threads
3. **Database Connection**: Sling connects to the DuckDB database file specified in the `path` parameter

### Template Variable Usage

The YAML templates use the `env` variable (not `dagster_env`) to conditionally load configurations:

- **`defs.yaml`**: `{{ 'replication_local.yaml' if env == 'LOCAL' else 'replication_prod.yaml' }}`
- **`profiles.yml`**: `{{ 'local' if env == 'LOCAL' else 'prod' }}`

### How to Change the DuckDB Database File

To modify the DuckDB database file location:

1. Edit the [`local`](dbt/jdbt/profiles.yml:3) target in [`profiles.yml`](dbt/jdbt/profiles.yml:3)
2. Update the `path` parameter to your desired file location
3. Example:
   ```yaml
   local:
     type: duckdb
     path: /your/custom/path/database.duckdb  # Change this path
     threads: 1
   ```

## Adding New Environments

To add a new environment:

1. Create a new replication configuration file (e.g., `replication_staging.yaml`)
2. Add the new target to `profiles.yml`
3. Update `defs.yaml` to conditionally load the new configuration
4. Update `profiles.yml` target selection logic

## Troubleshooting

### Common Issues

1. **Environment not switching**: Ensure the `.env` file is in the project root and the `DAGSTER_ENV` variable is set correctly.

2. **Databricks connection issues**: Verify that `DATABRICKS_TOKEN` is set as an environment variable when using PROD environment.

3. **DuckDB file permissions**: Ensure the `/tmp/` directory is writable for LOCAL environment.

### Verification

To verify the environment is set correctly:

1. Check the `.env` file contents
2. Verify the correct replication configuration is loaded in Dagster UI
3. Confirm the DBT profile target matches your environment