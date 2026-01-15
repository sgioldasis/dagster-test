# Project Summary: dbt-cloud-orchestration

## Overview

This Dagster project implements an ELT pipeline that orchestrates data ingestion via DLT, transformations with dbt Cloud, and downstream analytics. The pipeline is designed for scalability and observability.

## Architecture

The project uses a multi-code-location architecture in Dagster:

### 1. Ingestion Location (`src/dbt_cloud_orchestration/defs/ingestion/`)
- **Single DLT Asset**: `dlt_kaizen_wars_fact_virtual`
- **Data Source**: CSV file (`data/raw_fact_virtual.csv`) loaded into Databricks
- **Freshness Policy**: 1-minute lag tolerance (LegacyFreshnessPolicy)
- **Schedule**: Runs every 5 minutes (`kaizen_wars_dlt_schedule`)
- **No Sensors**: Manual control with UI-based freshness monitoring

### 2. DBT Location (`src/dbt_cloud_orchestration/defs/dbt/`)
- **DBT Cloud Assets**: Auto-discovered models from dbt Cloud project
- **Key Asset**: `stg_kaizen_wars__fact_virtual` (depends on DLT asset)
- **Automation**: `AutomationCondition.any_deps_updated()` for dependency-based triggering
- **Sensors**: `dbt_cloud_polling_sensor` for monitoring run status, `default_automation_sensor` for evaluation
- **Job**: `dbt_cloud_job_trigger` for manual/API-triggered runs

### 3. Downstream Location (`src/dbt_cloud_orchestration/defs/downstream/`)
- **Analytics Asset**: `fact_virtual_count_asset` for record aggregation
- **Dependency**: Relies on transformed dbt data

## Key Components

### Data Flow
1. **Ingestion**: CSV → Databricks via DLT
2. **Transformation**: dbt Cloud jobs process raw data
3. **Analytics**: Downstream assets perform aggregations

### Configuration
- **Environment Variables**: Databricks, dbt Cloud credentials, file paths
- **Workspace**: `workspace.yaml` defines code locations
- **Dependencies**: Dagster ecosystem (dagster, dagster-dbt, dagster-dlt, pandas, etc.)

### Freshness & Monitoring
- **Ingestion Asset**: UI-based freshness indicators (no sensors)
- **DBT Assets**: Automated evaluation via sensors
- **Visual Feedback**: Color-coded status in Dagster lineage view

## Usage

### Running the Pipeline
```bash
# Start Dagster UI
task dg-dev -- dbt-cloud-orchestration

# Or directly
cd /path/to/project && dg dev --workspace workspace.yaml
```

### Key Assets
- `dlt_kaizen_wars_fact_virtual`: Ingests game data
- `stg_kaizen_wars__fact_virtual`: Transforms data
- `fact_virtual_count_asset`: Aggregates counts

### Schedules
- `kaizen_wars_dlt_schedule`: Triggers ingestion every 5 minutes

## Current State

The project has been streamlined to focus on the core ELT functionality:
- Single DLT asset with proper freshness monitoring
- Integrated dbt Cloud orchestration
- Clean architecture with separated concerns
- Manual control with automated freshness evaluation
- No redundant sensors or schedules

## Dependencies & Environment

- **Python**: 3.10-3.14
- **Key Libraries**: Dagster, DLT, dbt Cloud integration
- **Infrastructure**: Databricks for data warehouse, dbt Cloud for transformations
- **Configuration**: Environment variables for all external services

This implementation provides a robust foundation for data pipeline orchestration with excellent monitoring and scalability capabilities.</content>
<parameter name="filePath">/Users/s.gioldasis-si/Projects/dagster-test/dbt-cloud-orchestration/SUMMARY.md