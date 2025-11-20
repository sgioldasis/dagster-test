import os
import time
import requests # Import the requests library

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
# 2. THE WORKAROUND OP (manual instantiation)
# ==============================================================================
@dg.op
def trigger_dbt_cloud_run_manual(context: dg.OpExecutionContext) -> int:
    """
    Manually triggers a dbt Cloud run using the correct API endpoint
    and polls for completion.
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

    # --- 1. Trigger the dbt Cloud run (using the correct endpoint) ---
    context.log.info(f"Triggering dbt Cloud job {job_id}...")
    
    # FIX: Use the /jobs/{job_id}/run/ endpoint
    trigger_url = f"{workspace.credentials.access_url.rstrip('/')}/api/v2/accounts/{workspace.credentials.account_id}/jobs/{job_id}/run/"
    headers = {"Authorization": f"Token {workspace.credentials.token}"}
    
    # FIX: The payload only needs the 'cause'
    payload = {
        "cause": f"Triggered by Dagster job '{context.job_name}'",
    }

    try:
        response = requests.post(trigger_url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an exception for bad status codes (4xx or 5xx)
        run_data = response.json()
        run_id = run_data["data"]["id"]
        context.log.info(f"Successfully triggered dbt Cloud run. Run ID: {run_id}")
    except requests.exceptions.RequestException as e:
        raise dg.Failure(f"Failed to trigger dbt Cloud run: {e}")

    # --- 2. Poll for the run to finish (this part was already correct) ---
    context.log.info(f"Polling for run {run_id} to complete...")
    poll_url = f"{workspace.credentials.access_url.rstrip('/')}/api/v2/accounts/{workspace.credentials.account_id}/runs/{run_id}/"
    
    while True:
        try:
            response = requests.get(poll_url, headers=headers)
            response.raise_for_status()
            run_data = response.json()
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

        except requests.exceptions.RequestException as e:
            context.log.error(f"Error polling for run status: {e}")
            time.sleep(30) # Wait longer on error

    return run_id


# ==============================================================================
# 3. THE JOB (no resource dependencies)
# ==============================================================================
@dg.job
def dbt_cloud_trigger_job():
    trigger_dbt_cloud_run_manual()


# # Diagnostic op to inspect the manually created workspace
# @dg.op
# def inspect_dbt_cloud_client(context: dg.OpExecutionContext):
#     """
#     A diagnostic op to inspect the client object returned by get_client().
#     """
#     # Get all required values directly from environment variables
#     account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
#     access_url = os.getenv("DBT_CLOUD_ACCESS_URL")
#     token = os.getenv("DBT_CLOUD_TOKEN")
#     project_id = int(os.getenv("DBT_CLOUD_PROJECT_ID"))
#     environment_id = int(os.getenv("DBT_CLOUD_ENVIRONMENT_ID"))

#     creds = DbtCloudCredentials(
#         account_id=account_id,
#         access_url=access_url,
#         token=token,
#     )
    
#     # Create the workspace configuration object
#     dbt_cloud_workspace = DbtCloudWorkspace(
#         credentials=creds,
#         project_id=project_id,
#         environment_id=environment_id,
#     )

#     # Get the client object that will perform the actions
#     client = dbt_cloud_workspace.get_client()
    
#     context.log.info(f"Type of client object: {type(client)}")
    
#     # Get all methods that don't start with an underscore
#     methods = [method for method in dir(client) if not method.startswith('_')]
    
#     context.log.info("Methods on the Dbt Cloud client:")
#     for method in sorted(methods):
#         context.log.info(f"- {method}")

# # Update the job to use the new op
# @dg.job
# def inspection_job():
#     inspect_dbt_cloud_client()

# ==============================================================================
# 4. TOP-LEVEL DEFINITIONS (no resource definitions)
# ==============================================================================
# defs = dg.Definitions(
#     assets=dbt_cloud_assets,
#     jobs=[dbt_cloud_trigger_job, inspection_job],
#     sensors=[dbt_cloud_sensor],
#     # No resources are defined here anymore.
# )

# The Definitions object must include BOTH jobs
defs = dg.Definitions(
    assets=dbt_cloud_assets,
    # jobs=[dbt_cloud_trigger_job, inspection_job],  # <-- Make sure inspection_job is here
    jobs=[dbt_cloud_trigger_job],  # <-- Make sure inspection_job is here
    sensors=[dbt_cloud_sensor],
    # No resources are defined here anymore.
)