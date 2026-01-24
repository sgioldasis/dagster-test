import dlt
from dagster import EnvVar
import os
import getpass
from .resources import DatabricksCredentials, PostgresCredentials


from .utils import get_postgres_connection_string, get_supabase_connection_string, parse_postgres_connection_string


def get_pipeline(credentials: DatabricksCredentials | None = None):
    """Create and return the DLT pipeline (Databricks destination)."""

    if credentials is None:
        print(
            "[DEBUG] Using duckdb destination for local testing (no Databricks credentials)"
        )
        return dlt.pipeline(
            pipeline_name="kaizen_wars_ingestion",
            destination=dlt.destinations.duckdb(),
            dataset_name=os.environ.get("DATABRICKS_SCHEMA", "main"),
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


def get_supabase_pipeline():
    """Create DLT pipeline with Supabase PostgreSQL destination (legacy)."""
    conn_string = get_supabase_connection_string()
    print("[DEBUG] Creating Supabase pipeline")
    return dlt.pipeline(
        pipeline_name="csv_to_supabase",
        destination=dlt.destinations.postgres(conn_string),
        dataset_name="public",
        dev_mode=False,
    )
