"""Dagster Resources for database connections.

This module defines ConfigurableResource classes that provide typed, validated
connections to external systems (Databricks and PostgreSQL).

What are Dagster Resources?
    Resources are dependencies that assets and ops need to execute.
    They provide:
    - Configuration validation (via Pydantic)
    - Connection pooling and lifecycle management
    - Separation of credentials from business logic

Usage Pattern:
    @asset
    def my_asset(context: AssetExecutionContext, databricks: DatabricksCredentials):
        client = databricks.get_client()
        # Use client...

Configuration:
    Resources are configured at definition time, either:
    - Programmatically in definitions.py
    - Via environment variables
    - In Dagster's UI for specific deployments
"""

import dagster as dg
from pydantic import Field

from ingestion.defs.utils import get_postgres_connection_string


class DatabricksCredentials(dg.ConfigurableResource):
    """Credentials and configuration for connecting to Databricks.

    This resource provides a typed interface for Databricks connection settings
    and includes helper methods to get SDK clients and connection dictionaries.

    Attributes:
        host: The Databricks workspace host (e.g., adb-xxx.azuredatabricks.net)
        token: Personal access token for authentication
        warehouse_id: SQL warehouse ID (used to construct http_path)
        http_path: Direct JDBC/ODBC HTTP path (alternative to warehouse_id)
        catalog: Unity Catalog name (default: "test")
        schema_name: Schema within the catalog (default: "main")
        notebook_job_id: Optional default job ID for triggering notebooks

    Example:
        ```python
        databricks = DatabricksCredentials(
            host="adb-123.azuredatabricks.net",
            token=dg.EnvVar("DATABRICKS_TOKEN"),
            warehouse_id="abc123def456",
            catalog="production",
            schema_name="analytics",
        )
        ```
    """

    host: str = Field(
        description="Databricks workspace host (e.g., adb-xxx.azuredatabricks.net)"
    )
    token: str = Field(
        description="Databricks personal access token"
    )
    warehouse_id: str | None = Field(
        default=None,
        description="SQL warehouse ID (used to construct http_path if not provided)"
    )
    http_path: str | None = Field(
        default=None,
        description="JDBC/ODBC HTTP path (e.g., /sql/1.0/warehouses/xxx)"
    )
    catalog: str = Field(
        default="test",
        description="Unity Catalog name"
    )
    schema_name: str = Field(
        default="main",
        description="Schema name within the catalog"
    )
    notebook_job_id: str | None = Field(
        default=None,
        description="Default notebook job ID to trigger for orchestration"
    )

    def get_client(self) -> "DatabricksClient":
        """Get the Databricks SDK client wrapper.

        Returns:
            DatabricksClient with a workspace_client attribute for API calls.

        Example:
            ```python
            client_wrapper = databricks.get_client()
            client = client_wrapper.workspace_client
            jobs = client.jobs.list()
            ```
        """
        from dagster_databricks.databricks import DatabricksClient

        return DatabricksClient(
            host=self.host,
            token=self.token,
        )

    def get_connection_dict(self) -> dict:
        """Get connection parameters as a dictionary for Sling.

        This format is used when configuring Sling connections programmatically.
        The http_path is auto-constructed from warehouse_id if not provided.

        Returns:
            Dictionary with host, token, http_path, and catalog.
        """
        http_path = self.http_path
        if not http_path and self.warehouse_id:
            http_path = f"/sql/1.0/warehouses/{self.warehouse_id}"

        return {
            "host": self.host,
            "token": self.token,
            "http_path": http_path,
            "catalog": self.catalog,
        }


class PostgresCredentials(dg.ConfigurableResource):
    """Credentials and configuration for connecting to PostgreSQL.

    This resource provides connection details for PostgreSQL databases.
    It supports both explicit field configuration and environment-based
    connection string resolution via the utils module.

    Priority for connection:
    1. If all fields (host, user, database) are provided, use them directly
    2. Otherwise, fall back to get_postgres_connection_string() which checks
       INGESTION_TARGET environment variable for local vs Supabase config

    Attributes:
        host: PostgreSQL server hostname
        port: Server port (default: 5432)
        user: Database username
        password: Database password (optional)
        database: Database name

    Example:
        ```python
        # Explicit configuration
        postgres = PostgresCredentials(
            host="localhost",
            user="postgres",
            database="mydb",
            password="secret",
        )

        # Or use environment-based resolution
        postgres = PostgresCredentials()  # Reads from env vars
        ```
    """

    host: str | None = Field(
        default=None,
        description="PostgreSQL server hostname"
    )
    port: str | None = Field(
        default=None,
        description="PostgreSQL server port"
    )
    user: str | None = Field(
        default=None,
        description="Database username"
    )
    password: str | None = Field(
        default=None,
        description="Database password"
    )
    database: str | None = Field(
        default=None,
        description="Database name"
    )

    def get_connection_string(self) -> str:
        """Get the PostgreSQL connection string.

        Constructs a connection string from explicit fields if all required
        fields are present. Otherwise, delegates to get_postgres_connection_string()
        which supports switching between local and Supabase via INGESTION_TARGET.

        Returns:
            PostgreSQL connection string in the format:
            postgresql://user:password@host:port/database
        """
        if all([self.host, self.user, self.database]):
            pwd = f":{self.password}" if self.password else ""
            port = f":{self.port}" if self.port else ""
            return f"postgresql://{self.user}{pwd}@{self.host}{port}/{self.database}"

        # Fall back to environment-based configuration
        return get_postgres_connection_string()


# Export list for clean imports
__all__ = ["DatabricksCredentials", "PostgresCredentials"]
