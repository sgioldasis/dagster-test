import os
import time
import json
import urllib.request
import urllib.error

import dagster as dg
from dagster_dbt.cloud_v2.resources import (
    DbtCloudCredentials,
    DbtCloudWorkspace,
    load_dbt_cloud_asset_specs,
)
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor

# ==============================================================================
# 1. WORKSPACE FOR DEFINITIONS (runs at load time)
# ==============================================================================
# This part is unchanged. It's used to discover assets and sensors.
# We still need the hardcoded workspace for this.

project_id = int(os.getenv("DBT_CLOUD_PROJECT_ID"))
environment_id = int(os.getenv("DBT_CLOUD_ENVIRONMENT_ID"))

creds = DbtCloudCredentials(
    account_id=os.getenv("DBT_CLOUD_ACCOUNT_ID"),
    access_url=os.getenv("DBT_CLOUD_ACCESS_URL"),
    token=os.getenv("DBT_CLOUD_TOKEN"),
)

workspace_for_defs = DbtCloudWorkspace(
    credentials=creds,
    project_id=project_id,
    environment_id=environment_id,
)

dbt_cloud_assets = load_dbt_cloud_asset_specs(workspace=workspace_for_defs)
dbt_cloud_sensor = build_dbt_cloud_polling_sensor(workspace=workspace_for_defs)


# ==============================================================================
# 2. THE FINAL OP (using urllib)
# ==============================================================================
@dg.op
def trigger_dbt_cloud_run_manual(context: dg.OpExecutionContext) -> int:
    """
    Triggers a dbt Cloud run and polls for completion using the
    built-in `urllib` library. This is a robust, dependency-free solution.
    """
    # Get the job_id from the op's run configuration
    job_id = context.op_config["job_id"]
    
    # Use the DbtCloudWorkspace to get our configuration values cleanly
    workspace = DbtCloudWorkspace(
        credentials=DbtCloudCredentials(
            account_id=os.getenv("DBT_CLOUD_ACCOUNT_ID"),
            access_url=os.getenv("DBT_CLOUD_ACCESS_URL"),
            token=os.getenv("DBT_CLOUD_TOKEN"),
        ),
        project_id=int(os.getenv("DBT_CLOUD_PROJECT_ID")),
        environment_id=int(os.getenv("DBT_CLOUD_ENVIRONMENT_ID")),
    )

    # --- 1. Trigger the dbt Cloud run ---
    context.log.info(f"Triggering dbt Cloud job {job_id}...")
    
    # Use the correct endpoint that we know works
    trigger_url = f"{workspace.credentials.access_url.rstrip('/')}/api/v2/accounts/{workspace.credentials.account_id}/jobs/{job_id}/run/"
    headers = {
        "Authorization": f"Token {workspace.credentials.token}",
        "Content-Type": "application/json",
    }
    payload = {
        "cause": f"Triggered by Dagster job '{context.job_name}'",
    }

    try:
        # Prepare the request using urllib
        payload_bytes = json.dumps(payload).encode('utf-8')
        request = urllib.request.Request(trigger_url, data=payload_bytes, headers=headers, method='POST')
        
        with urllib.request.urlopen(request) as response:
            run_data = json.loads(response.read().decode('utf-8'))
            run_id = run_data["data"]["id"]
            context.log.info(f"Successfully triggered dbt Cloud run. Run ID: {run_id}")

    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        raise dg.Failure(f"Failed to trigger dbt Cloud run: {e}")

    # --- 2. Poll for the run to finish ---
    context.log.info(f"Polling for run {run_id} to complete...")
    poll_url = f"{workspace.credentials.access_url.rstrip('/')}/api/v2/accounts/{workspace.credentials.account_id}/runs/{run_id}/"
    
    while True:
        try:
            # Prepare the GET request for polling
            poll_request = urllib.request.Request(poll_url, headers=headers, method='GET')
            
            with urllib.request.urlopen(poll_request) as response:
                run_data = json.loads(response.read().decode('utf-8'))
                status = run_data["data"]["status_humanized"]
                
                context.log.info(f"Run {run_id} status: {status}")

                if status in ["Success", "Completed"]:
                    context.log.info(f"dbt Cloud run {run_id} completed successfully.")
                    break
                elif status in ["Failed", "Cancelled", "Error"]:
                    raise dg.Failure(
                        description=f"dbt Cloud run {run_id} failed with status: {status}",
                        metadata={"Run ID": run_id, "Status": status},
                    )
                
                time.sleep(15)  # Wait for 15 seconds before polling again

        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            context.log.error(f"Error polling for run status: {e}")
            time.sleep(30) # Wait longer on error

    return run_id


# ==============================================================================
# 3. THE JOB
# ==============================================================================
@dg.job
def dbt_cloud_trigger_job():
    trigger_dbt_cloud_run_manual()


# ==============================================================================
# 4. TOP-LEVEL DEFINITIONS
# ==============================================================================
defs = dg.Definitions(
    assets=dbt_cloud_assets,
    jobs=[dbt_cloud_trigger_job],
    sensors=[dbt_cloud_sensor],
)