"""DLT pipeline definitions for ingestion.

This module defines dlt pipelines for loading data to PostgreSQL and Databricks.
It creates pipeline instances that are used by the Dagster dlt component.
"""

import os
from pathlib import Path

import dlt
from dlt.destinations import postgres, databricks
from dotenv import load_dotenv

from . import dlt_pipeline

# Load environment variables for local development
load_dotenv()


# =============================================================================
# PostgreSQL Pipeline (CSV → PostgreSQL)
# =============================================================================

def _get_postgres_credentials_dict() -> dict:
    """Get PostgreSQL credentials as a dictionary for DLT destination.

    Returns:
        Dictionary with PostgreSQL connection parameters.
    """
    # Check for explicit connection string first
    conn_string = os.environ.get("POSTGRES_CONNECTION_STRING")
    
    if conn_string:
        # Parse connection string into dict format that DLT expects
        from urllib.parse import urlparse
        parsed = urlparse(conn_string)
        return {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "database": parsed.path.lstrip("/") or "postgres",
            "username": parsed.username or "postgres",
            "password": parsed.password or "",  # Allow empty password
        }
    
    # Build from individual env vars
    return {
        "host": os.environ.get("LOCAL_POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("LOCAL_POSTGRES_PORT", "5432")),
        "database": os.environ.get("LOCAL_POSTGRES_DATABASE", "postgres"),
        "username": os.environ.get("LOCAL_POSTGRES_USER", "postgres"),
        "password": os.environ.get("LOCAL_POSTGRES_PASSWORD", ""),
    }


def _get_postgres_credentials() -> str:
    """Get PostgreSQL connection string for DLT sql_database source.

    The sql_table function requires a connection string format, not a dict.

    Returns:
        PostgreSQL connection string.
    """
    # Check for explicit connection string first
    conn_string = os.environ.get("POSTGRES_CONNECTION_STRING")
    
    if conn_string:
        return conn_string
    
    # Build from individual env vars
    host = os.environ.get("LOCAL_POSTGRES_HOST", "localhost")
    port = os.environ.get("LOCAL_POSTGRES_PORT", "5432")
    database = os.environ.get("LOCAL_POSTGRES_DATABASE", "postgres")
    username = os.environ.get("LOCAL_POSTGRES_USER", "postgres")
    password = os.environ.get("LOCAL_POSTGRES_PASSWORD", "")
    
    if password:
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    else:
        return f"postgresql://{username}@{host}:{port}/{database}"


# Alias for backward compatibility
def _get_postgres_credentials_for_destination() -> dict:
    """Get credentials dict for DLT destination (pipeline)."""
    return _get_postgres_credentials_dict()


# Pipeline for loading CSV data to PostgreSQL
postgres_pipeline = dlt.pipeline(
    pipeline_name="csv_to_postgres",
    destination=postgres(credentials=_get_postgres_credentials_dict()),
    dataset_name="public",
)

# Source instance for CSV data (creates fact_virtual resource)
csv_source = dlt_pipeline.csv_source()


# =============================================================================
# Databricks Pipeline (PostgreSQL → Databricks)
# =============================================================================

def _get_databricks_credentials() -> dict:
    """Get Databricks credentials from environment.

    Returns:
        Dictionary with Databricks connection parameters.
    """
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    http_path = os.environ.get(
        "DATABRICKS_HTTP_PATH",
        f"/sql/1.0/warehouses/{warehouse_id}" if warehouse_id else ""
    )

    return {
        "server_hostname": os.environ.get("DATABRICKS_HOST", ""),
        "http_path": http_path,
        "access_token": os.environ.get("DATABRICKS_TOKEN", ""),
        "catalog": os.environ.get("DATABRICKS_CATALOG", "test"),
        "schema": os.environ.get("DATABRICKS_SCHEMA", "main"),
    }


# Pipeline for loading PostgreSQL data to Databricks
# The destination table will be: test.main.dlt_fact_virtual
# (catalog and schema are configured in _get_databricks_credentials)
databricks_pipeline = dlt.pipeline(
    pipeline_name="postgres_to_databricks",
    destination=databricks(credentials=_get_databricks_credentials()),
    dataset_name="main",
)


# =============================================================================
# SQL Table Source for PostgreSQL (used for Databricks ingestion)
# =============================================================================

def _create_postgres_fact_virtual_source():
    """Create a dlt source for reading dlt_fact_virtual from PostgreSQL.

    This source reads data from PostgreSQL that will be loaded into Databricks.

    Returns:
        DLT source with sql_table resource for dlt_fact_virtual table.
    """
    from dlt.sources.sql_database import sql_table

    # Create the sql_table resource with write_disposition set to replace
    dlt_fact_virtual_table = sql_table(
        table="dlt_fact_virtual",
        schema="public",
        credentials=_get_postgres_credentials(),
    )
    # Set write disposition to replace for full-refresh behavior
    dlt_fact_virtual_table.write_disposition = "replace"

    # Wrap it in a source
    @dlt.source(name="postgres_source")
    def postgres_source():
        yield dlt_fact_virtual_table

    return postgres_source()


# SQL table source instance for PostgreSQL dlt_fact_virtual table
# This is created at import time - the table must exist
postgres_fact_virtual_source = _create_postgres_fact_virtual_source()
