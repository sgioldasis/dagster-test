# src/dbt_cloud_orchestration/defs/dbt/resources.py
"""Resources for the dbt code location."""

import dagster as dg
from pydantic import Field


class DbtCloudCredentials(dg.ConfigurableResource):
    """Credentials for connecting to dbt Cloud."""

    account_id: int = Field(description="dbt Cloud account ID")
    access_url: str = Field(description="dbt Cloud access URL")
    token: str = Field(description="dbt Cloud API token")
    project_id: int = Field(description="dbt Cloud project ID")
    environment_id: int = Field(description="dbt Cloud environment ID")
    job_id: int | None = Field(default=None, description="dbt Cloud job ID")


__all__ = ["DbtCloudCredentials"]
