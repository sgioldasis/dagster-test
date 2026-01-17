import os
import dlt

from .resources import DatabricksCredentials


def _get_databricks_credentials() -> dict:
    """Get Databricks connection credentials from environment variables."""
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    databricks_catalog = os.getenv("DATABRICKS_CATALOG", "test")

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


pipeline = dlt.pipeline(
    pipeline_name="kaizen_wars_ingestion",
    destination=dlt.destinations.databricks(
        credentials=_get_databricks_credentials(),
    ),
    dataset_name=os.getenv("DATABRICKS_SCHEMA", "main"),
    dev_mode=False,
)
