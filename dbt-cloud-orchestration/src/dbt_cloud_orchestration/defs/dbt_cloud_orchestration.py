# src/dbt_cloud_orchestration/defs/dbt_cloud_orchestration.py

import os
import dagster as dg
from dagster_dbt.cloud_v2.asset_decorator import dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor

# Define credentials
creds = DbtCloudCredentials(
    account_id=os.getenv("DBT_CLOUD_ACCOUNT_ID", "413"),
    access_url=os.getenv("DBT_CLOUD_ACCESS_URL", "https://tw590.eu1.dbt.com"),
    token=os.getenv("DBT_CLOUD_TOKEN"),
)

# Define the workspace
workspace = DbtCloudWorkspace(
    credentials=creds,
    project_id=os.getenv("DBT_CLOUD_PROJECT_ID", "10157"),
    environment_id=os.getenv("DBT_CLOUD_ENVIRONMENT_ID", "26829"),
)

# Builds your asset graph
@dbt_cloud_assets(workspace=workspace)
def my_dbt_cloud_assets(
    context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace
):
    yield from dbt_cloud.cli(args=["build"], context=context).wait()

# Automate your assets
my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(
    lambda spec: spec.replace_attributes(
        automation_condition=dg.AutomationCondition.eager()
    )
)

# Creates the sensor - CRITICAL: Must be assigned to a variable!
dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)