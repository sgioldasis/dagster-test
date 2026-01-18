import dlt
from dagster import EnvVar

from .resources import DatabricksCredentials


def get_databricks_credentials() -> dict:
    """Get Databricks connection credentials using DatabricksCredentials resource.

    This function creates a DatabricksCredentials instance from environment variables
    and returns a dictionary compatible with DLT.
    """
    credentials = DatabricksCredentials(
        host=EnvVar("DATABRICKS_HOST").get_value() or "",
        token=EnvVar("DATABRICKS_TOKEN").get_value() or "",
        warehouse_id=EnvVar("DATABRICKS_WAREHOUSE_ID").get_value(),
        http_path=EnvVar("DATABRICKS_HTTP_PATH").get_value(),
        catalog=EnvVar("DATABRICKS_CATALOG").get_value() or "test",
        schema_name=EnvVar("DATABRICKS_SCHEMA").get_value() or "main",
    )
    return credentials.get_connection_dict()


pipeline = dlt.pipeline(
    pipeline_name="kaizen_wars_ingestion",
    destination=dlt.destinations.databricks(
        credentials=get_databricks_credentials(),
    ),
    dataset_name=EnvVar("DATABRICKS_SCHEMA").get_value() or "main",
    dev_mode=False,
)
