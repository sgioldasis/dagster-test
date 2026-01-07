# dbt_cloud_orchestration

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server with the multi-code-location workspace:

```bash
dg dev --workspace workspace.yaml
```

This will load the three separate code locations:
- `ingestion` - DLT Databricks pipelines
- `dbt` - DBT Cloud assets and jobs
- `downstream` - Downstream processing assets

Open http://localhost:3000 in your browser to see the project.

> **Note**: If you want to use the original single code location configuration, you can rename `definitions_backup.py` back to `definitions.py` and run `dg dev` without the workspace flag.

## Project Structure

This project has been reorganized into **three separate Dagster code locations** for better separation of concerns:

```
dbt-cloud-orchestration/
├── workspace.yaml                  # Workspace configuration for 3 code locations
├── definitions_backup.py          # Original single code location (backup)
├── src/dbt_cloud_orchestration/
│   ├── defs/
│   │   ├── ingestion/              # 📦 Code Location 1: Ingestion
│   │   │   ├── definitions.py      # DLT Databricks pipelines
│   │   │   └── dlt_pipeline.py      # Moved DLT pipeline
│   │   │
│   │   ├── dbt/                   # 📦 Code Location 2: DBT
│   │   │   └── definitions.py      # DBT Cloud assets
│   │   │
│   │   ├── downstream/            # 📦 Code Location 3: Downstream
│   │   │   ├── definitions.py      # Downstream assets
│   │   │   └── fact_virtual_count.py # Moved downstream asset
│   │   │
│   │   └── dbt_cloud_orchestration.py  # Updated DBT assets
```

### Code Locations Overview

1. **Ingestion** (`ingestion`)
   - **Purpose**: Data ingestion pipelines
   - **Contents**: DLT Databricks assets, schedules
   - **Assets**: `dlt_databricks_assets`, `kaizen_wars_ingest_assets`
   - **Schedules**: `kaizen_wars_dlt_schedule`

2. **DBT** (`dbt`)
   - **Purpose**: DBT Cloud assets and jobs
   - **Contents**: DBT Cloud assets, sensors, jobs
   - **Assets**: `my_dbt_cloud_assets`, `kaizen_wars_assets`
   - **Sensors**: `dbt_cloud_polling_sensor`
   - **Jobs**: `dbt_cloud_job_trigger`

3. **Downstream** (`downstream`)
   - **Purpose**: Downstream processing
   - **Contents**: Post-processing assets
   - **Assets**: `fact_virtual_count_asset`

## Key Benefits

### ✨ Advantages of the Multi-Code-Location Architecture

1. **Clear Separation of Concerns**
   - Ingestion, DBT, and downstream processing are logically separated
   - Each code location has a single, well-defined responsibility
   - Easier to understand and maintain the project structure

2. **Team Independence**
   - Different teams can work on their respective code locations independently
   - Changes in one area don't affect other areas
   - Reduced risk of merge conflicts and deployment issues

3. **Improved Performance**
   - Smaller code locations load faster in the Dagster UI
   - Faster development iteration cycles
   - Better resource utilization

4. **Enhanced Maintainability**
   - Easier to locate and manage related components
   - Clear ownership boundaries
   - Simplified debugging and testing

5. **Scalable Architecture**
   - Ready for team growth and additional components
   - Easy to add new code locations as needed
   - Supports microservices-like development approach

6. **Better Organization**
   - Logical grouping of related assets, jobs, and schedules
   - Clear separation between data ingestion, transformation, and processing
   - Improved code navigation and discovery

7. **Flexible Deployment**
   - Code locations can be deployed independently if needed
   - Supports different deployment strategies for different components
   - Easier to manage environment-specific configurations

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
