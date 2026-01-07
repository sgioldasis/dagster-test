# src/dbt_cloud_orchestration/definitions.py

import dagster as dg
from dbt_cloud_orchestration.defs.dbt_cloud_orchestration import (
    my_dbt_cloud_assets,
    dbt_cloud_polling_sensor,
    workspace,
    dbt_cloud_job_trigger,
    kaizen_wars_assets,
)
from dbt_cloud_orchestration.defs.ingestion.dlt_pipeline import (
    dlt_databricks_assets,
    kaizen_wars_ingest_assets,
    kaizen_wars_dlt_schedule,
)
from dagster_dlt import DagsterDltResource
from dagster import AutomationConditionSensorDefinition

# Explicitly define the automation sensor to ensure conditions are evaluated
automation_sensor = AutomationConditionSensorDefinition(
    name="default_automation_sensor",
    target=dg.AssetSelection.all(),  # Target all assets safely
    use_user_code_server=True,
)

defs = dg.Definitions(
    assets=[
        my_dbt_cloud_assets,
        dlt_databricks_assets,
        *kaizen_wars_assets,
        kaizen_wars_ingest_assets,
    ],
    sensors=[dbt_cloud_polling_sensor, automation_sensor],
    schedules=[kaizen_wars_dlt_schedule],
    jobs=[dbt_cloud_job_trigger],
    resources={
        "dbt_cloud": workspace,
        "dlt": DagsterDltResource(),
    },
)