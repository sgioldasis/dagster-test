import dagster as dg
from pydantic import Field
from .utils import get_postgres_connection_string


class DatabricksCredentials(dg.ConfigurableResource):
    """Credentials for connecting to Databricks, providing both SDK and DLT connection info."""

    host: str = Field(description="Databricks host")
    token: str = Field(description="Databricks access token")
    warehouse_id: str | None = Field(
        default=None, description="Databricks SQL warehouse ID"
    )
    http_path: str | None = Field(
        default=None, description="Databricks HTTP path for JDBC connection"
    )
    catalog: str = Field(default="test", description="Databricks catalog name")
    schema_name: str = Field(
        default="main", description="Databricks schema/dataset name"
    )
    retry_policy: str | None = Field(
        default=None,
        description="Optional retry policy name for DLT operations",
    )
    write_max_concurrent_runs: int = Field(
        default=5, description="Maximum concurrent DLT pipeline runs"
    )

    def get_client(self):
        """Get the Databricks SDK client wrapper."""
        from dagster_databricks.databricks import DatabricksClient
        return DatabricksClient(
            host=self.host,
            token=self.token,
        )

    def get_connection_dict(self) -> dict:
        """Get connection dictionary for DLT."""
        http_path = self.http_path
        if not http_path and self.warehouse_id:
            http_path = f"/sql/1.0/warehouses/{self.warehouse_id}"

        return {
            "host": self.host,
            "token": self.token,
            "http_path": http_path,
            "catalog": self.catalog,
        }

    def get_pipeline_settings(self) -> dict:
        """Get DLT pipeline settings."""
        settings: dict = {
            "write_max_concurrent_runs": self.write_max_concurrent_runs,
        }
        if self.retry_policy:
            settings["retry_policy"] = self.retry_policy
        return settings


class PostgresCredentials(dg.ConfigurableResource):
    """Credentials for connecting to PostgreSQL."""

    host: str | None = Field(default=None)
    port: str | None = Field(default=None)
    user: str | None = Field(default=None)
    password: str | None = Field(default=None)
    database: str | None = Field(default=None)

    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        if all([self.host, self.user, self.database]):
            pwd = f":{self.password}" if self.password else ""
            port = f":{self.port}" if self.port else ""
            return f"postgresql://{self.user}{pwd}@{self.host}{port}/{self.database}"
        return get_postgres_connection_string()


__all__ = ["DatabricksCredentials", "PostgresCredentials"]
