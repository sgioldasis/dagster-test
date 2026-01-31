"""DBT Cloud code location."""

import dagster as dg
from dagster import AutomationConditionSensorDefinition, EnvVar, SourceAsset
from dotenv import load_dotenv

load_dotenv()

from .defs.assets import create_dbt_cloud_definitions
from .defs.resources import DbtCloudCredentials, DbtCloudRunConfig
from .defs.constants import (
    DEFAULT_MAX_CONCURRENT_RUNS,
    DEFAULT_RUN_TIMEOUT_SECONDS,
    DEFAULT_RETRY_FAILED_RUNS,
)


automation_sensor = AutomationConditionSensorDefinition(
    name="default_automation_sensor",
    target=dg.AssetSelection.all(),
    use_user_code_server=True,
)

dbt_cloud_run_config = DbtCloudRunConfig(
    max_concurrent_runs=int(
        EnvVar("DBT_CLOUD_MAX_CONCURRENT_RUNS").get_value() or DEFAULT_MAX_CONCURRENT_RUNS
    ),
    timeout_seconds=int(
        EnvVar("DBT_CLOUD_RUN_TIMEOUT_SECONDS").get_value() or DEFAULT_RUN_TIMEOUT_SECONDS
    ),
    retry_failed_runs=(EnvVar("DBT_CLOUD_RETRY_FAILED").get_value() or "").lower()
    == "true",
)

dbt_cloud_job_id = EnvVar("DBT_CLOUD_JOB_ID").get_value()
dbt_cloud_credentials = DbtCloudCredentials(
    account_id=int(EnvVar("DBT_CLOUD_ACCOUNT_ID").get_value() or "0"),
    access_url=EnvVar("DBT_CLOUD_ACCESS_URL").get_value() or "https://cloud.getdbt.com",
    token=EnvVar("DBT_CLOUD_TOKEN").get_value() or "",
    project_id=int(EnvVar("DBT_CLOUD_PROJECT_ID").get_value() or "0"),
    environment_id=int(EnvVar("DBT_CLOUD_ENVIRONMENT_ID").get_value() or "0"),
    job_id=int(dbt_cloud_job_id) if dbt_cloud_job_id else None,
    run_timeout_seconds=int(
        EnvVar("DBT_CLOUD_RUN_TIMEOUT_SECONDS").get_value() or DEFAULT_RUN_TIMEOUT_SECONDS
    ),
)

fact_virtual_source = SourceAsset(
    key="fact_virtual",
    description="Asset from ingestion code location",
    group_name="ingestion",
)


# Create dbt Cloud definitions
dbt_definitions = create_dbt_cloud_definitions(dbt_cloud_credentials, dbt_cloud_run_config)

my_dbt_cloud_assets = dbt_definitions.assets
dbt_cloud_polling_sensor = dbt_definitions.sensor
workspace = dbt_definitions.workspace
dbt_cloud_job_trigger = dbt_definitions.job_trigger
kaizen_wars_assets = dbt_definitions.additional_assets

defs = dg.Definitions(
    assets=[
        my_dbt_cloud_assets,
        *kaizen_wars_assets,
        fact_virtual_source,
    ],
    sensors=[dbt_cloud_polling_sensor, automation_sensor],
    resources={
        "dbt_cloud": workspace,
    },
    jobs=[dbt_cloud_job_trigger] if dbt_cloud_job_trigger else [],
)


# Kept for backward compatibility with tools that expect a function attribute
def dbt_defs() -> dg.Definitions:
    """Wrapper function for tools that expect a function attribute."""
    return defs
