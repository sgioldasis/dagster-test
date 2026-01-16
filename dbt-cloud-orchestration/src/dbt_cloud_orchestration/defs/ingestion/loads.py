# src/dbt_cloud_orchestration/defs/ingestion/loads.py
"""DLT pipeline configuration for Databricks ingestion."""

import os
import dlt

from . import dlt_pipeline


def _get_databricks_credentials() -> dict:
    """Get Databricks connection credentials from environment variables."""
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    databricks_catalog = os.getenv("DATABRICKS_CATALOG", "test")

    # Infer http_path from warehouse_id if not explicitly set
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    if not http_path and warehouse_id:
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    return {
        "server_hostname": databricks_host,
        "access_token": databricks_token,
        "http_path": http_path,
        "catalog": databricks_catalog,
    }


# Configure DLT pipeline for Databricks
pipeline = dlt.pipeline(
    pipeline_name="kaizen_wars_ingestion",
    destination=dlt.destinations.databricks(
        credentials=_get_databricks_credentials(),
    ),
    dataset_name=os.getenv("DATABRICKS_SCHEMA", "main"),
    dev_mode=False,  # Disable dev_mode to keep stable dataset names
)

# Instantiate the source for the component
kaizen_wars_source = dlt_pipeline.kaizen_wars_source()
