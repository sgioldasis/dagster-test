# src/dbt_cloud_orchestration/defs/dbt/definitions.py
"""
DBT Cloud code location.

This code location is independent but depends on:
- dlt_kaizen_wars_fact_virtual (from ingestion code location)

Cross-location dependencies are declared via SourceAsset to avoid
coupling between code locations.
"""

import dagster as dg
from dagster import AutomationConditionSensorDefinition, EnvVar, SourceAsset

from .assets import create_dbt_cloud_definitions
from .resources import DbtCloudCredentials


automation_sensor = AutomationConditionSensorDefinition(
    name="default_automation_sensor",
    target=dg.AssetSelection.all(),
    use_user_code_server=True,
)

dbt_cloud_credentials = DbtCloudCredentials(
    account_id=int(EnvVar("DBT_CLOUD_ACCOUNT_ID").get_value() or "0"),
    access_url=EnvVar("DBT_CLOUD_ACCESS_URL").get_value() or "",
    token=EnvVar("DBT_CLOUD_TOKEN").get_value() or "",
    project_id=int(EnvVar("DBT_CLOUD_PROJECT_ID").get_value() or "0"),
    environment_id=int(EnvVar("DBT_CLOUD_ENVIRONMENT_ID").get_value() or "0"),
    job_id=int(EnvVar("DBT_CLOUD_JOB_ID").get_value())
    if EnvVar("DBT_CLOUD_JOB_ID").get_value()
    else None,
)

# External asset reference - declare dependency without importing
# This asset lives in the "ingestion" code location
dlt_kaizen_wars_fact_virtual_source = SourceAsset(
    key="dlt_kaizen_wars_fact_virtual",
    description="DLT asset from ingestion code location",
)


@dg.definitions
def dbt_defs() -> dg.Definitions:
    (
        my_dbt_cloud_assets,
        dbt_cloud_polling_sensor,
        workspace,
        dbt_cloud_job_trigger,
        kaizen_wars_assets,
    ) = create_dbt_cloud_definitions(dbt_cloud_credentials)

    return dg.Definitions(
        assets=[
            my_dbt_cloud_assets,
            *kaizen_wars_assets,
            dlt_kaizen_wars_fact_virtual_source,
        ],
        sensors=[dbt_cloud_polling_sensor, automation_sensor],
        resources={
            "dbt_cloud": workspace,
        },
    )
