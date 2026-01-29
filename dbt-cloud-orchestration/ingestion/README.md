# Ingestion with Dagster + Sling

This project demonstrates how to use Dagster with Sling for data ingestion. It uses a hybrid approach combining YAML-based components for Sling replications and programmatic assets for external orchestration.

## What is Sling?

[Sling](https://slingdata.io/) is a CLI tool for moving data between databases and storage systems. It supports:

- **Sources**: CSV, JSON, PostgreSQL, MySQL, SQL Server, Oracle, and more
- **Targets**: PostgreSQL, Databricks, Snowflake, BigQuery, Redshift, and more

## Architecture

```
CSV Files (source)
    │
    ▼
┌─────────────────────┐
│ csv_fact_virtual    │  Sling: CSV → PostgreSQL
│ (Sling Component)   │
└─────────────────────┘
    │
    ▼
┌─────────────────────────┐
│ databricks_fact_virtual │  Sling: PostgreSQL → Databricks
│ (Sling Component)       │
└─────────────────────────┘
    │
    ▼ (optional)
┌──────────────────────────┐
│ run_databricks_job       │  Programmatic: Trigger Databricks notebook
│ (Programmatic Asset)     │
└──────────────────────────┘
```

## Project Structure

```
ingestion/
├── ingestion/
│   ├── __init__.py              # Package exports
│   ├── definitions.py           # Main Dagster definitions
│   ├── components/              # Component-based assets
│   │   └── ingestion_sling/     # Sling replication component
│   │       ├── component.py     # Custom component implementation
│   │       ├── component.yaml   # Connection configuration
│   │       ├── replication_csv.yaml   # CSV → Postgres replication
│   │       └── replication_db.yaml    # Postgres → Databricks replication
│   └── defs/                    # Programmatic definitions
│       ├── resources.py         # Database connection resources
│       └── utils.py             # Connection string utilities
├── pyproject.toml               # Project dependencies
├── workspace.yaml               # Dagster workspace configuration
└── dagster.yaml                 # Dagster instance settings
```

## Running Locally

### 1. Setup Environment

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration/ingestion
uv sync
```

### 2. Configure Environment Variables

Create a `.env` file:

```bash
# PostgreSQL (local)
LOCAL_POSTGRES_HOST=localhost
LOCAL_POSTGRES_PORT=5432
LOCAL_POSTGRES_USER=postgres
LOCAL_POSTGRES_PASSWORD=your_password
LOCAL_POSTGRES_DATABASE=postgres

# Or for Supabase
INGESTION_TARGET=supabase
SUPABASE_HOST=your-project.supabase.co
SUPABASE_USER=postgres
SUPABASE_PASSWORD=your_password

# Databricks (optional)
DATABRICKS_HOST=adb-xxx.azuredatabricks.net
DATABRICKS_TOKEN=your-token
DATABRICKS_WAREHOUSE_ID=xxx
DATABRICKS_CATALOG=test
DATABRICKS_SCHEMA=main
```

### 3. Start Dagster

```bash
dg dev --workspace workspace.yaml
```

Open http://localhost:3000 to view the ingestion assets.

## Key Components

### 1. SlingReplicationCollectionComponent

The [`IngestionSlingComponent`](ingestion/components/ingestion_sling/component.py:42) extends Dagster's `SlingReplicationCollectionComponent` to:

- Load replication configurations from YAML files
- Resolve environment variables in connection strings
- Customize asset keys and dependencies
- Apply freshness policies per asset

### 2. Replication YAML Files

Replication specs define data movement operations:

**replication_csv.yaml** - CSV to PostgreSQL:
```yaml
source: CSV_SOURCE
target: POSTGRES_DEST
defaults:
  mode: full-refresh
streams:
  /path/to/file.csv:
    object: public.table_name
    meta:
      dagster:
        asset_key: "custom_asset_name"
        group: "ingestion"
```

**replication_db.yaml** - PostgreSQL to Databricks:
```yaml
source: POSTGRES_DEST
target: DATABRICKS_TARGET
streams:
  public.table_name:
    object: target_table
    meta:
      dagster:
        asset_key: "databricks_asset"
        upstream_assets: ["source_asset"]
```

### 3. Custom Translator

The [`IngestionSlingTranslator`](ingestion/components/ingestion_sling/component.py:19) customizes how Sling streams become Dagster assets:

```python
class IngestionSlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition):
        spec = super().get_asset_spec(stream_definition)
        return spec.replace_attributes(
            key=AssetKey(path),
            group_name="ingestion",
            automation_condition=AutomationCondition.eager(),
            freshness_policy=FreshnessPolicy.time_window(...),
        )
```

## Adding New Replications

### Step 1: Add Connection (if needed)

Edit [`component.yaml`](ingestion/components/ingestion_sling/component.yaml):

```yaml
connections:
  MY_NEW_SOURCE:
    type: postgres
    connection_string: env:MY_CONN_STRING
```

### Step 2: Create Replication YAML

Create a new file like `replication_new.yaml`:

```yaml
source: MY_NEW_SOURCE
target: POSTGRES_DEST
defaults:
  mode: full-refresh
streams:
  schema.source_table:
    object: schema.target_table
    meta:
      dagster:
        asset_key: "my_new_asset"
        upstream_assets: ["dependency_asset"]
        group: "ingestion"
```

### Step 3: Register Replication

Add to [`component.yaml`](ingestion/components/ingestion_sling/component.yaml):

```yaml
replications:
  - path: replication_csv.yaml
  - path: replication_db.yaml
  - path: replication_new.yaml  # Add here
```

## Resources

### DatabricksCredentials

Configuration for Databricks connections:

```python
databricks = DatabricksCredentials(
    host="adb-xxx.azuredatabricks.net",
    token=EnvVar("DATABRICKS_TOKEN"),
    warehouse_id="xxx",
    catalog="test",
    schema_name="main",
)
```

### PostgresCredentials

Configuration for PostgreSQL:

```python
postgres = PostgresCredentials(
    host="localhost",
    user="postgres",
    database="mydb",
)
# Or use environment-based resolution:
postgres = PostgresCredentials()
```

## Automation & Scheduling

### Eager Automation

Assets use `AutomationCondition.eager()` to automatically run when upstream dependencies materialize:

```python
automation_condition=AutomationCondition.eager()
```

### Freshness Policies

Assets have freshness policies for data quality monitoring:

```python
freshness_policy=FreshnessPolicy.time_window(
    fail_window=timedelta(minutes=5),
    warn_window=timedelta(minutes=2),
)
```

### Sensors

- **default_automation_sensor**: Triggers assets based on automation conditions
- **freshness_sensor**: Monitors asset freshness and raises alerts

## Learn More

- [Sling Documentation](https://docs.slingdata.io/)
- [Dagster Sling Integration](https://docs.dagster.io/integrations/sling)
- [Dagster Components](https://docs.dagster.io/concepts/components)
