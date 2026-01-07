# src/dbt_cloud_orchestration/defs/dbt_cloud_orchestration.py

import os
import time
import json

import dagster as dg
from dagster_dbt.cloud_v2.asset_decorator import dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor
# Add these imports
from dagster import job, op, asset
from dagster_dbt.cloud_v2.resources import DbtCloudJob
import requests

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
# Builds your asset graph
# Builds your asset graph
# Builds your asset graph
# Builds your asset graph
@dbt_cloud_assets(workspace=workspace)
def my_dbt_cloud_assets(
    context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace
):
    yield from dbt_cloud.cli(args=["build"], context=context).wait(timeout=600)

# Conditional Logic: Check if stg_kaizen_wars__fact_virtual is discovered
discovered_asset_keys = {k.path[-1] for k in my_dbt_cloud_assets.keys}
target_key = "stg_kaizen_wars__fact_virtual"
is_discovered = target_key in discovered_asset_keys

kaizen_wars_assets = []

if is_discovered:
    # Case A: Asset exists. Map it to add deps/automation.
    def map_dbt_specs(spec: dg.AssetSpec) -> dg.AssetSpec:
        # Default automation for all
        spec = spec.replace_attributes(automation_condition=dg.AutomationCondition.any_deps_updated())
        
        if spec.key.path[-1] == target_key:
            return spec.replace_attributes(
                deps=[*spec.deps, dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])],
                automation_condition=dg.AutomationCondition.any_deps_updated()
            )
        return spec
    
    my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(map_dbt_specs)

else:
    # Case B: Asset missing. Define explicitly AND map defaults for others.
    my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=dg.AutomationCondition.any_deps_updated())
    )
    
    @asset(
        key=[target_key],
        deps=[dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])],
        automation_condition=dg.AutomationCondition.any_deps_updated(),
        compute_kind="dbt",
        group_name="kaizen_wars"
    )
    def stg_kaizen_wars__fact_virtual(context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace):
        """
        Formally managed dbt asset for Kaizen Wars fact_virtual (Explicit Definition).
        """
        yield from dbt_cloud.cli(args=["build", "--select", target_key], context=context).wait(timeout=600)
        
    kaizen_wars_assets = [stg_kaizen_wars__fact_virtual]

# Create an op that triggers a specific dbt Cloud job via API
@op
def trigger_dbt_cloud_job_api(context: dg.OpExecutionContext):
    """Trigger a specific dbt Cloud job directly via API"""
    job_id = int(os.getenv("DBT_CLOUD_JOB_ID", "493899"))
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID", "413")
    token = os.getenv("DBT_CLOUD_TOKEN")
    access_url = os.getenv("DBT_CLOUD_ACCESS_URL", "https://tw590.eu1.dbt.com")
    
    context.log.info(f"Triggering dbt Cloud job {job_id}")
    
    # Trigger the job
    url = f"{access_url}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.post(url, headers=headers, json={"cause": "Triggered from Dagster"})
    response.raise_for_status()
    
    run_data = response.json()["data"]
    run_id = run_data["id"]
    context.log.info(f"Job triggered, run ID: {run_id}")
    
    # Poll for completion
    status_url = f"{access_url}/api/v2/accounts/{account_id}/runs/{run_id}/"
    
    while True:
        status_resp = requests.get(status_url, headers=headers)
        status_resp.raise_for_status()
        
        run_data = status_resp.json()["data"]
        status = run_data["status"]
        
        # Status codes: 1=queued, 2=starting, 3=running, 10=success, 20=error, 30=cancelled
        if status in [1, 2, 3]:
            context.log.info(f"Job still running, status: {status}")
            time.sleep(10)
            continue
        
        context.log.info(f"Job completed with status: {status}")
        return {"run_id": run_id, "status": status, "run_data": run_data}

@job
def dbt_cloud_job_trigger():
    trigger_dbt_cloud_job_api()


# Creates the sensor - CRITICAL: Must be assigned to a variable!
dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)

# Export everything
__all__ = [
    "my_dbt_cloud_assets",
    "dbt_cloud_polling_sensor",
    "workspace",
    "dbt_cloud_job_trigger",
    "kaizen_wars_assets",
]