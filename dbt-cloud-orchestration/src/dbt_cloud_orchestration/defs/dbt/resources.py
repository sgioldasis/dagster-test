# src/dbt_cloud_orchestration/defs/dbt/resources.py
"""Resources for the dbt code location."""

import dagster as dg
from pydantic import Field


class DbtCloudCredentials(dg.ConfigurableResource):
    """Credentials for connecting to dbt Cloud."""

    account_id: int = Field(description="dbt Cloud account ID")
    access_url: str = Field(
        default="https://cloud.getdbt.com", description="dbt Cloud access URL"
    )
    token: str = Field(description="dbt Cloud API token")
    project_id: int = Field(description="dbt Cloud project ID")
    environment_id: int = Field(description="dbt Cloud environment ID")
    job_id: int | None = Field(
        default=None, description="dbt Cloud job ID for triggering"
    )
    run_timeout_seconds: int = Field(
        default=600, description="Timeout for dbt Cloud run operations"
    )
    polling_interval_seconds: int = Field(
        default=30, description="Polling interval for run status checks"
    )


class DbtCloudRunConfig(dg.ConfigurableResource):
    """Configuration for dbt Cloud run behavior."""

    max_concurrent_runs: int = Field(
        default=3, description="Max concurrent runs per job"
    )
    timeout_seconds: int = Field(default=1800, description="Default run timeout")
    retry_failed_runs: bool = Field(default=False, description="Auto-retry failed runs")
    notify_on_failure: str | None = Field(
        default=None, description="Email to notify on failure"
    )


__all__ = ["DbtCloudCredentials", "DbtCloudRunConfig"]
