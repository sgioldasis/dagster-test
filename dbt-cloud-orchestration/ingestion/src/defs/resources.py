# ingestion/src/ingestion/defs/resources.py
"""Resources for the ingestion code location."""

import dagster as dg
from pydantic import Field


class DatabricksCredentials(dg.ConfigurableResource):
    """Credentials for connecting to Databricks."""

    host: str = Field(
        description="Databricks host (e.g.,dbc-xxxxx.cloud.databricks.com)"
    )
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

    def get_connection_dict(self) -> dict:
        """Get connection dictionary for DLT."""
        http_path = self.http_path
        if not http_path and self.warehouse_id:
            http_path = f"/sql/1.0/warehouses/{self.warehouse_id}"

        return {
            "server_hostname": self.host,
            "access_token": self.token,
            "http_path": http_path,
            "catalog": self.catalog,
            "schema": self.schema_name,
        }

    def get_pipeline_settings(self) -> dict:
        """Get DLT pipeline settings."""
        settings = {
            "write_max_concurrent_runs": self.write_max_concurrent_runs,
        }
        if self.retry_policy:
            settings["retry_policy"] = self.retry_policy
        return settings


__all__ = ["DatabricksCredentials"]
