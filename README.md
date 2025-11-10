# Project description

A small test project for experimenting with Dagster pipelines and local development workflows using Devbox. The repository contains example Dagster jobs, Dagster component configuration for local runs, and supporting scripts to make it easy to iterate on pipeline code.

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



