# src/dbt_cloud_orchestration/defs/ingestion/definitions.py

import dagster as dg
from dbt_cloud_orchestration.defs.ingestion.dlt_pipeline import (
    kaizen_wars_ingest_assets,
    kaizen_wars_dlt_schedule,
)

from dagster_dlt import DagsterDltResource
from dagster import AutomationConditionSensorDefinition


# Ingestion-specific Definitions
defs = dg.Definitions(
    assets=[
        kaizen_wars_ingest_assets,
    ],
    schedules=[kaizen_wars_dlt_schedule],
    sensors=[],
    resources={
        "dlt": DagsterDltResource(),
    },
)
