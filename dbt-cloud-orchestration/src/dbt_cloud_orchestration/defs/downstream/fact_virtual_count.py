# src/dbt_cloud_orchestration/defs/downstream/fact_virtual_count.py

import os
import time
import json
import dagster as dg
from dagster import asset
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
import requests

@asset(
    deps=[dg.AssetKey("stg_kaizen_wars__fact_virtual")],
    description="Asset that counts records in stg_kaizen_wars__fact_virtual and writes to file",
    automation_condition=dg.AutomationCondition.any_deps_updated()
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
    # Example of what you might do:
    # metadata_url = f"{access_url}/api/v2/accounts/{account_id}/metadata/"
    # metadata_response = requests.get(metadata_url, headers={"Authorization": f"Bearer {token}"})
    
    # For now, return a placeholder count
    return 12345  # Replace with actual count from API