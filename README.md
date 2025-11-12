# Project description

This project is a testbed for experimenting with Dagster pipelines and local development workflows using Devbox. It provides example Dagster jobs, component configurations, and scripts to streamline pipeline development and iteration. The repository demonstrates how to ingest data from CSV files, orchestrate data pipelines with Dagster, and integrate with dbt for data transformation. It also includes instructions for setting up the environment, running the Dagster UI, and managing data with tools like Sling and Podman. The project is ideal for learning and prototyping modern data engineering workflows in a local environment.

## Prerequisites
- Environment variables
    - In your .bashrc or .zshrc add the following:
    ```
    export DATABRICKS_TOKEN=<your databricks token>
    ```
- Install Devbox (https://www.jetify.com/devbox)


## Setup (quick)

1. Enter devbox shell:
   ```bash
   devbox shell
   ```

2. (optional) Initialize Podman VM (one-time):
   ```bash
   podman machine init
   ```

3. Run Dagster UI:
   ```bash
   cd my-project
   dg dev
   ```
   After that you can view the Dagster UI by pointing your browser at http://127.0.0.1:3000


## Repository layout
- dagster_test/ — Dagster pipelines, solids/ops, and jobs
- tests/ — unit tests for pipeline code
- requirements.txt — Python dependencies

## Useful commands

### Postgres

```bash
# Create the jaffle_shop database
createdb jaffle_shop

# Connect to the jaffle_shop database
psql -h localhost -d jaffle_shop
```

### Sling

```bash
# List connections
sling conns list

# Load data from local CSV files into PostgreSQL
sling run \
  --src-conn "file://data/raw_customers.csv" \
  --tgt-conn POSTGRES_CONN \
  --tgt-object public.raw_customers

sling run \
  --src-conn "file://data/raw_orders.csv" \
  --tgt-conn POSTGRES_CONN \
  --tgt-object public.raw_orders

sling run \
  --src-conn "file://data/raw_payments.csv" \
  --tgt-conn POSTGRES_CONN \
  --tgt-object public.raw_payments

```

### Podman

```bash
#This command downloads the necessary files and creates a lightweight Linux VM for Podman to use. You only need to do this once.
podman machine init
```


## Authoring

```bash
devbox shell
uvx create-dagster@latest project dlt-project
cd dlt-project

# Download th csv fils
curl -O https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_payments.csv
curl -O https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_orders.csv
curl -O https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv

# Add Sling
uv add dagster-sling

# List the components
dg list components

# Scaffold the ingestion using Sling
dg scaffold defs dagster_sling.SlingReplicationCollectionComponent ingestion
# Note: This will create defs at dlt-project/src/dlt_project/defs/ingestion.
# Then we edit the defs

# We check the yaml 
dg check yaml

# We list the defs
dg list defs

# We can now launch the new assets
dg launch --assets target/main/raw_customers,target/main/raw_orders,target/main/raw_payments

# We can check the data is in Duckdb
duckdb /tmp/jaffle_platform.duckdb -c "SELECT * FROM raw_orders LIMIT 5"

# Now we clone the dbt project
git clone --depth=1 https://github.com/dagster-io/jaffle-platform.git dbt && rm -rf dbt/.git

# We add the dagster and duckdb dbt dependencies
uv add dagster-dbt dbt-duckdb

# List the components
dg list components

# Scaffold the integration with the dbt project
dg scaffold defs dagster_dbt.DbtProjectComponent jdbt --project-path dbt/jdbt
# Note: This will create defs at dlt-project/src/dlt_project/defs/jdbt.
# Then we edit the created defs. 
# We add translation to align the asset keys with the Sling asset keys

# We check the yaml 
dg check yaml

# We list the defs
dg list defs

# We can now launch the new assets
dg launch --assets target/main/stg_payments

# We can check the data is in Duckdb
duckdb /tmp/jaffle_platform.duckdb -c "SELECT * FROM stg_payments LIMIT 5"

