# src/dbt_cloud_orchestration/defs/ingestion/definitions.py

import dagster as dg
from dbt_cloud_orchestration.defs.ingestion.dlt_pipeline import (
    dlt_databricks_assets,
    kaizen_wars_ingest_assets,
    kaizen_wars_dlt_schedule,
)
from dagster_dlt import DagsterDltResource

# Ingestion-specific Definitions
defs = dg.Definitions(
    assets=[
        dlt_databricks_assets,
        kaizen_wars_ingest_assets,
    ],
    schedules=[kaizen_wars_dlt_schedule],
    resources={
        "dlt": DagsterDltResource(),
    },
)