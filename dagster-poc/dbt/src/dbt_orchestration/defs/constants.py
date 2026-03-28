"""Constants for dbt Cloud orchestration."""

from enum import IntEnum


class DbtCloudRunStatus(IntEnum):
    """dbt Cloud run status codes.
    
    See: https://docs.getdbt.com/docs/dbt-cloud-apis/run-statuses
    """
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30
    
    @classmethod
    def is_running(cls, status: int) -> bool:
        """Check if status indicates a running state."""
        return status in (cls.QUEUED, cls.STARTING, cls.RUNNING)
    
    @classmethod
    def is_terminal(cls, status: int) -> bool:
        """Check if status indicates a terminal state."""
        return status in (cls.SUCCESS, cls.ERROR, cls.CANCELLED)


# Default configuration values
DEFAULT_RUN_TIMEOUT_SECONDS = 1800  # 30 minutes
DEFAULT_MAX_CONCURRENT_RUNS = 3
DEFAULT_POLLING_INTERVAL_SECONDS = 30
DEFAULT_RETRY_FAILED_RUNS = False
DEFAULT_MAX_POLLING_ITERATIONS = 360  # 30 minutes / 5 second polling = 360 iterations
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 5

# Asset configuration
DEFAULT_KAIZEN_WARS_TARGET_KEY = "stg_kaizen_wars__fact_virtual"
DEFAULT_KAIZEN_WARS_PACKAGE = "dbt_optimove"
DEFAULT_FRESHNESS_DEADLINE_CRON = "*/1 * * * *"
DEFAULT_FRESHNESS_LOWER_BOUND_MINUTES = 1
