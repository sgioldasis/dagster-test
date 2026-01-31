"""Resources for the dbt code location."""

import dagster as dg
from pydantic import Field

from .constants import (
    DEFAULT_MAX_CONCURRENT_RUNS,
    DEFAULT_POLLING_INTERVAL_SECONDS,
    DEFAULT_RETRY_FAILED_RUNS,
    DEFAULT_RUN_TIMEOUT_SECONDS,
)


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
        default=DEFAULT_RUN_TIMEOUT_SECONDS,
        description="Timeout for dbt Cloud run operations",
    )


class DbtCloudRunConfig(dg.ConfigurableResource):
    """Configuration for dbt Cloud run behavior."""

    max_concurrent_runs: int = Field(
        default=DEFAULT_MAX_CONCURRENT_RUNS,
        description="Max concurrent runs per job",
    )
    timeout_seconds: int = Field(
        default=DEFAULT_RUN_TIMEOUT_SECONDS,
        description="Default run timeout",
    )
    retry_failed_runs: bool = Field(
        default=DEFAULT_RETRY_FAILED_RUNS,
        description="Auto-retry failed runs",
    )
    notify_on_failure: str | None = Field(
        default=None, description="Email to notify on failure"
    )
    polling_interval_seconds: int = Field(
        default=DEFAULT_POLLING_INTERVAL_SECONDS,
        description="Polling interval for run status checks",
    )
    max_retries: int = Field(
        default=3,
        description="Max retries for API calls",
    )
    retry_delay_seconds: int = Field(
        default=5,
        description="Delay between retries in seconds",
    )


__all__ = ["DbtCloudCredentials", "DbtCloudRunConfig"]
