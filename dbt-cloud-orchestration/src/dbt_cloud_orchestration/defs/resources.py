import dagster as dg
from pydantic import Field
from dagster import EnvVar


class DataPathConfig(dg.ConfigurableResource):
    """Configuration for data file paths."""

    fact_virtual_path: str = Field(description="Path to fact virtual data CSV")


class DbtCloudCredentials(dg.ConfigurableResource):
    """Credentials for connecting to dbt Cloud."""

    account_id: int = Field(description="dbt Cloud account ID")
    access_url: str = Field(description="dbt Cloud access URL")
    token: str = Field(description="dbt Cloud API token")
    project_id: int = Field(description="dbt Cloud project ID")
    environment_id: int = Field(description="dbt Cloud environment ID")
    job_id: int | None = Field(default=None, description="dbt Cloud job ID")


class DatabricksCredentials(dg.ConfigurableResource):
    """Credentials for connecting to Databricks."""

    host: str = Field(description="Databricks host")
    token: str = Field(description="Databricks access token")
    warehouse_id: str | None = Field(
        default=None, description="Databricks warehouse ID"
    )
    http_path: str | None = Field(default=None, description="Databricks HTTP path")
    catalog: str = Field(default="test", description="Databricks catalog")
    dataset_name: str = Field(
        default="main", description="Databricks schema/dataset name"
    )

    def get_credentials_dict(self) -> dict:
        """Get credentials dictionary for DLT."""
        http_path = self.http_path
        if not http_path and self.warehouse_id:
            http_path = f"/sql/1.0/warehouses/{self.warehouse_id}"

        return {
            "server_hostname": self.host,
            "access_token": self.token,
            "http_path": http_path,
            "catalog": self.catalog,
        }
