import dlt
from dagster import EnvVar
import os
import getpass
from .resources import DatabricksCredentials, PostgresCredentials


from .utils import get_postgres_connection_string, parse_postgres_connection_string


def get_pipeline(credentials: DatabricksCredentials | None = None):
    """Create and return the DLT pipeline (Databricks destination)."""

    if credentials is None:
        print(
            "[DEBUG] Using duckdb destination for local testing (no Databricks credentials)"
        )
        return dlt.pipeline(
            pipeline_name="kaizen_wars_ingestion",
            destination=dlt.destinations.duckdb(),
            dataset_name=EnvVar("DATABRICKS_SCHEMA").get_value() or "main",
            dev_mode=True,
        )

    print("[DEBUG] Creating Databricks pipeline")
    return dlt.pipeline(
        pipeline_name="kaizen_wars_ingestion",
        destination=dlt.destinations.databricks(
            credentials=credentials.get_connection_dict(),
            truncate_tables_on_staging_destination_before_load=False,
        ),
        dataset_name=credentials.schema_name,
        dev_mode=False,
    )


def get_postgres_pipeline(credentials: PostgresCredentials | None = None):
    """Create DLT pipeline with local PostgreSQL destination."""
    if credentials:
        conn_string = credentials.get_connection_string()
    else:
        conn_string = get_postgres_connection_string()
        
    print(f"[DEBUG] Creating local PostgreSQL pipeline with: {conn_string}")

    parsed_creds = parse_postgres_connection_string(conn_string)

    return dlt.pipeline(
        pipeline_name="csv_to_postgres",
        destination=dlt.destinations.postgres(credentials=parsed_creds),
        dataset_name="public",
        dev_mode=False,
    )


def create_databricks_pipeline_from_env():
    """Create Databricks pipeline using environment variables (for decorators)."""
    host = EnvVar("DATABRICKS_HOST").get_value() or "<host>"
    token = EnvVar("DATABRICKS_TOKEN").get_value() or "<token>"
    warehouse_id = EnvVar("DATABRICKS_WAREHOUSE_ID").get_value()
    http_path = EnvVar("DATABRICKS_HTTP_PATH").get_value()

    if not http_path and warehouse_id:
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    return dlt.pipeline(
        pipeline_name="kaizen_wars_ingestion",
        destination=dlt.destinations.databricks(
            credentials={
                "host": host,
                "token": token,
                "http_path": http_path or "<http_path>",
                "catalog": EnvVar("DATABRICKS_CATALOG").get_value() or "test",
            }
        ),
        dataset_name=EnvVar("DATABRICKS_SCHEMA").get_value() or "main",
        dev_mode=True,
    )


def create_postgres_pipeline_from_env():
    """Create PostgreSQL pipeline using environment variables (for decorators)."""
    conn_string = get_postgres_connection_string()
    creds = parse_postgres_connection_string(conn_string)

    return dlt.pipeline(
        pipeline_name="csv_to_postgres",
        destination=dlt.destinations.postgres(credentials=creds),
        dataset_name="public",
        dev_mode=True,
    )
