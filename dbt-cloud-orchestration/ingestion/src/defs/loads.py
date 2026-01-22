import dlt
from dagster import EnvVar
import os
import getpass
from .resources import DatabricksCredentials


def get_postgres_connection_string():
    """Build PostgreSQL connection string from env vars. Uses local Postgres by default."""
    host = EnvVar("LOCAL_POSTGRES_HOST").get_value() or "localhost"
    port = EnvVar("LOCAL_POSTGRES_PORT").get_value() or "5432"
    user = EnvVar("LOCAL_POSTGRES_USER").get_value() or getpass.getuser()
    password = EnvVar("LOCAL_POSTGRES_PASSWORD").get_value() or ""
    database = EnvVar("LOCAL_POSTGRES_DATABASE").get_value() or "postgres"

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        return f"postgresql://{user}@{host}:{port}/{database}"


def get_supabase_connection_string():
    """Build Supabase PostgreSQL connection string from env vars."""
    host = EnvVar("SUPABASE_HOST").get_value() or "aws-1-eu-west-1.pooler.supabase.com"
    port = EnvVar("SUPABASE_PORT").get_value() or "5432"
    user = EnvVar("SUPABASE_USER").get_value() or "postgres.optokmygftwwajposhdy"
    password = EnvVar("SUPABASE_PASSWORD").get_value() or "SupaBigbro14!!"
    database = EnvVar("SUPABASE_DATABASE").get_value() or "postgres"
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


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


def get_postgres_pipeline():
    """Create DLT pipeline with local PostgreSQL destination."""
    from urllib.parse import urlparse

    conn_string = get_postgres_connection_string()
    print(f"[DEBUG] Creating local PostgreSQL pipeline with: {conn_string}")

    # Parse connection string for credentials to pass directly to DLT
    parsed = urlparse(conn_string)

    credentials = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "username": parsed.username,
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/"),
    }

    return dlt.pipeline(
        pipeline_name="csv_to_postgres",
        destination=dlt.destinations.postgres(credentials=credentials),
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
