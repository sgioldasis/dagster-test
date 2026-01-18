import dlt
from dagster import EnvVar, String
import os


def get_databricks_credentials() -> dict | None:
    """Get Databricks connection credentials using environment variables."""
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    if not host or not token:
        print(
            f"[DEBUG] Missing Databricks credentials: host='{host}', token={'set' if token else 'not set'}"
        )
        return None

    credentials = {
        "server_hostname": host,
        "access_token": token,
        "http_path": os.environ.get("DATABRICKS_HTTP_PATH", ""),
        "catalog": os.environ.get("DATABRICKS_CATALOG", "test"),
        "schema": os.environ.get("DATABRICKS_SCHEMA", "main"),
    }
    print(f"[DEBUG] Databricks credentials configured for host: {host}")
    return credentials


def get_pipeline():
    """Create and return the DLT pipeline."""
    credentials = get_databricks_credentials()

    if credentials is None:
        print(
            "[DEBUG] Using duckdb destination for local testing (no Databricks credentials)"
        )
        return dlt.pipeline(
            pipeline_name="kaizen_wars_ingestion",
            destination=dlt.destinations.duckdb(
                initial_state=False,
            ),
            dataset_name=os.environ.get("DATABRICKS_SCHEMA", "main"),
            dev_mode=True,
        )

    return dlt.pipeline(
        pipeline_name="kaizen_wars_ingestion",
        destination=dlt.destinations.databricks(
            credentials=credentials,
        ),
        dataset_name=os.environ.get("DATABRICKS_SCHEMA", "main"),
        dev_mode=False,
    )


pipeline = get_pipeline()
