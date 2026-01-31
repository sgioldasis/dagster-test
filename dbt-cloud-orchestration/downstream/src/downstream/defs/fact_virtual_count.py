"""Asset that processes fact_virtual data from dbt Cloud runs."""

import json
import time
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from dagster import AssetExecutionContext, AssetKey, EnvVar, asset

# Constants
OUTPUT_DIR = Path(EnvVar("OUTPUT_DIR").get_value() or "./output")
DBT_CLOUD_ACCOUNT_ID = int(EnvVar("DBT_CLOUD_ACCOUNT_ID").get_value() or "0")
DBT_CLOUD_ACCESS_URL = EnvVar("DBT_CLOUD_ACCESS_URL").get_value() or ""
DBT_CLOUD_TOKEN = EnvVar("DBT_CLOUD_TOKEN").get_value() or ""

# Asset dependencies
FACT_VIRTUAL_ASSET_KEY = AssetKey(["dbt_optimove", "stg_kaizen_wars__fact_virtual"])


@asset(
    key=AssetKey(["downstream", "fact_virtual_count"]),
    deps=[FACT_VIRTUAL_ASSET_KEY],
    description="Asset that counts records in stg_kaizen_wars__fact_virtual and writes to file",
    automation_condition=dg.AutomationCondition.any_deps_updated(),
    group_name="downstream",
)
def fact_virtual_count_asset(context: AssetExecutionContext) -> Path:
    """Count records in fact_virtual table and write result to JSON file.

    This asset depends on stg_kaizen_wars__fact_virtual from the dbt code location
    and produces a JSON file containing the record count and metadata.

    Args:
        context: Dagster execution context for logging and metadata.

    Returns:
        Path to the output JSON file.

    Raises:
        ValueError: If required environment variables are not set.
        requests.RequestException: If API calls fail.
        RuntimeError: If no successful dbt Cloud runs are found.
    """
    # Validate configuration
    _validate_config(context)

    context.log.info("Fetching recent dbt Cloud runs to find fact table information...")

    # Find the most recent successful run
    successful_run_id = _find_successful_run_id(context)
    context.log.info(f"Using run ID: {successful_run_id}")

    # Get the fact table count (currently mocked)
    fact_table_count = _get_fact_table_count_from_api(
        DBT_CLOUD_ACCOUNT_ID,
        DBT_CLOUD_TOKEN,
        DBT_CLOUD_ACCESS_URL,
        context,
    )

    # Write output
    output_file = _write_count_output(
        fact_table_count=fact_table_count,
        run_id=successful_run_id,
    )

    context.log.info(f"Fact table count written to {output_file}: {fact_table_count}")

    # Add metadata to the asset materialization
    context.add_output_metadata({
        "record_count": fact_table_count,
        "run_id": successful_run_id,
        "output_file": str(output_file),
    })

    return output_file


def _validate_config(context: AssetExecutionContext) -> None:
    """Validate that required environment variables are set.

    Args:
        context: Dagster execution context for logging.

    Raises:
        ValueError: If required configuration is missing.
    """
    if not DBT_CLOUD_ACCOUNT_ID:
        raise ValueError("DBT_CLOUD_ACCOUNT_ID environment variable is required")
    if not DBT_CLOUD_ACCESS_URL:
        raise ValueError("DBT_CLOUD_ACCESS_URL environment variable is required")
    if not DBT_CLOUD_TOKEN:
        raise ValueError("DBT_CLOUD_TOKEN environment variable is required")

    context.log.debug("Configuration validated successfully")


def _find_successful_run_id(context: AssetExecutionContext) -> int:
    """Find the most recent successful dbt Cloud run ID.

    Args:
        context: Dagster execution context for logging.

    Returns:
        The ID of the most recent successful run.

    Raises:
        RuntimeError: If no successful runs are found.
        requests.RequestException: If the API request fails.
    """
    runs_url = f"{DBT_CLOUD_ACCESS_URL}/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/runs/"
    headers = {"Authorization": f"Bearer {DBT_CLOUD_TOKEN}"}

    try:
        runs_response = requests.get(
            runs_url,
            headers=headers,
            params={"limit": 5, "order_by": "-created_at"},
            timeout=30,
        )
        runs_response.raise_for_status()
    except requests.Timeout:
        context.log.error("Request to dbt Cloud API timed out")
        raise
    except requests.RequestException as e:
        context.log.error(f"Failed to fetch runs from dbt Cloud: {e}")
        raise

    runs_data = runs_response.json().get("data", [])

    for run in runs_data:
        # Status 10 = Success in dbt Cloud API
        if run.get("status") == 10:
            return run["id"]

    raise RuntimeError("No successful dbt Cloud runs found in the last 5 runs")


def _get_fact_table_count_from_api(
    account_id: int,
    token: str,
    access_url: str,
    context: AssetExecutionContext,
) -> int:
    """Get the count of records from the fact table via dbt Cloud API.

    This is currently a mock implementation returning a placeholder value.
    In production, this should query the actual data warehouse or dbt Cloud metadata.

    Args:
        account_id: dbt Cloud account ID.
        token: dbt Cloud API token.
        access_url: dbt Cloud access URL.
        context: Dagster execution context for logging.

    Returns:
        The count of records in the fact table.
    """
    context.log.warning("Using mock fact table count - implement actual query in production")
    return 12345


def _write_count_output(
    fact_table_count: int,
    run_id: int,
    fact_table_name: str = "stg_kaizen_wars__fact_virtual",
) -> Path:
    """Write the count output to a JSON file.

    Args:
        fact_table_count: The count of records.
        run_id: The dbt Cloud run ID.
        fact_table_name: Name of the fact table (default: stg_kaizen_wars__fact_virtual).

    Returns:
        Path to the written output file.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "fact_virtual_count.json"

    output_data: dict[str, Any] = {
        "fact_table": fact_table_name,
        "count": fact_table_count,
        "timestamp": time.time(),
        "run_id": run_id,
    }

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)

    return output_file


__all__ = ["fact_virtual_count_asset"]
