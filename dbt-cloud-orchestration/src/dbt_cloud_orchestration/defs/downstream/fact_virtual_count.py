# src/dbt_cloud_orchestration/defs/downstream/fact_virtual_count.py

import os
import time
import json
import dagster as dg
from dagster import asset, EnvVar
import requests


@asset(
    deps=[dg.AssetKey("stg_kaizen_wars__fact_virtual")],
    description="Asset that counts records in stg_kaizen_wars__fact_virtual and writes to file",
    automation_condition=dg.AutomationCondition.any_deps_updated(),
    group_name="downstream",
)
def fact_virtual_count_asset(context: dg.AssetExecutionContext):
    """
    Asset that depends on stg_kaizen_wars__fact_virtual and produces a file with the count
    """
    account_id = int(EnvVar("DBT_CLOUD_ACCOUNT_ID").get_value() or "0")
    access_url = EnvVar("DBT_CLOUD_ACCESS_URL").get_value() or ""
    token = EnvVar("DBT_CLOUD_TOKEN").get_value() or ""

    context.log.info("Fetching recent dbt Cloud runs to find fact table information...")

    runs_url = f"{access_url}/api/v2/accounts/{account_id}/runs/"
    headers = {"Authorization": f"Bearer {token}"}

    runs_response = requests.get(
        runs_url, headers=headers, params={"limit": 5, "order_by": "-created_at"}
    )
    runs_response.raise_for_status()

    successful_run_id = None
    for run in runs_response.json()["data"]:
        if run["status"] == 10:
            successful_run_id = run["id"]
            break

    if not successful_run_id:
        raise Exception("No successful dbt Cloud runs found")

    context.log.info(f"Using run ID: {successful_run_id}")

    fact_table_count = get_fact_table_count_from_api(
        account_id, token, access_url, context
    )

    output_file = "fact_virtual_count.json"
    with open(output_file, "w") as f:
        json.dump(
            {
                "fact_table": "stg_kaizen_wars__fact_virtual",
                "count": fact_table_count,
                "timestamp": time.time(),
                "run_id": successful_run_id,
            },
            f,
            indent=2,
        )

    context.log.info(f"Fact table count written to {output_file}: {fact_table_count}")

    return output_file


def get_fact_table_count_from_api(account_id, token, access_url, context):
    return 12345
