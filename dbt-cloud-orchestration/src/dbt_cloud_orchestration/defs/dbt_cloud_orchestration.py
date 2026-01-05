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


@asset(
    deps=["stg_kaizen_wars__fact_virtual"],
    description="Asset that counts records in stg_kaizen_wars__fact_virtual and writes to file",
    automation_condition=dg.AutomationCondition.eager()
)
def fact_virtual_count_asset(context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace):
    """
    Asset that depends on stg_kaizen_wars__fact_virtual and produces a file with the count
    """
    # Get the count using dbt Cloud API
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID", "413")
    token = os.getenv("DBT_CLOUD_TOKEN")
    access_url = os.getenv("DBT_CLOUD_ACCESS_URL", "https://tw590.eu1.dbt.com")
    
    # First, we need to get the run ID from the latest successful run
    # This assumes the fact table was created in a recent dbt Cloud run
    runs_url = f"{access_url}/api/v2/accounts/{account_id}/runs/"
    headers = {"Authorization": f"Bearer {token}"}
    
    context.log.info("Fetching recent dbt Cloud runs to find fact table information...")
    
    # Get recent runs
    runs_response = requests.get(runs_url, headers=headers, params={
        "limit": 5,
        "order_by": "-created_at"
    })
    runs_response.raise_for_status()
    
    # Look for a successful run that might contain our fact table
    successful_run_id = None
    for run in runs_response.json()["data"]:
        if run["status"] == 10:  # Status 10 = success
            successful_run_id = run["id"]
            break
    
    if not successful_run_id:
        raise Exception("No successful dbt Cloud runs found")
    
    context.log.info(f"Using run ID: {successful_run_id}")
    
    # Get run artifacts to find the fact table count
    # This is a simplified approach - in a real scenario, you might need to:
    # 1. Get the job ID from the run
    # 2. Get the job definition to find which models were run
    # 3. Query the database directly or use dbt Cloud's metadata API
    
    # For this example, we'll simulate getting the count
    # In a real implementation, you would query the actual database or use dbt Cloud metadata
    fact_table_count = get_fact_table_count_from_api(account_id, token, access_url, context)
    
    # Write the count to a file
    output_file = "fact_virtual_count.json"
    with open(output_file, "w") as f:
        json.dump({
            "fact_table": "stg_kaizen_wars__fact_virtual",
            "count": fact_table_count,
            "timestamp": time.time(),
            "run_id": successful_run_id
        }, f, indent=2)
    
    context.log.info(f"Fact table count written to {output_file}: {fact_table_count}")
    
    return output_file


def get_fact_table_count_from_api(account_id, token, access_url, context):
    """
    Helper function to get the count from dbt Cloud API
    Note: This is a simplified implementation. In reality, you might need to:
    1. Use dbt Cloud's metadata API if available
    2. Query the database directly
    3. Parse dbt Cloud job logs
    """
    # For this example, we'll return a simulated count
    # In a real implementation, you would implement actual API calls
    context.log.info("Getting fact table count from dbt Cloud...")
    
    # Simulated API call - replace with actual implementation
    # Example of what you might do:
    # metadata_url = f"{access_url}/api/v2/accounts/{account_id}/metadata/"
    # metadata_response = requests.get(metadata_url, headers={"Authorization": f"Bearer {token}"})
    
    # For now, return a placeholder count
    return 12345  # Replace with actual count from API



# Creates the sensor - CRITICAL: Must be assigned to a variable!
dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)

# Export everything
__all__ = [
    "my_dbt_cloud_assets",
    "dbt_cloud_polling_sensor",
    "workspace",
    "dbt_cloud_job_trigger",
    "fact_virtual_count_asset",
]