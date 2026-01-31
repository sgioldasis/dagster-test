"""Dagster Definitions for the Ingestion code location.

This module defines all Dagster assets, resources, sensors, and checks for the
ingestion pipeline. It uses a hybrid approach:

1. Component-based Sling assets (from YAML configuration)
2. Programmatic Databricks orchestration asset

Key Concepts:
- Sling: A data movement tool that syncs data between sources and targets
- Dagster Components: YAML-first configuration for common patterns
- Freshness Checks: Monitor data freshness and alert on stale data
- Automation: Eager execution when upstream dependencies update

Architecture:
    CSV files (source)
         │
         ▼
    ┌─────────────────┐
    │ csv_fact_virtual│  (Sling: CSV → PostgreSQL)
    └─────────────────┘
         │
         ▼
    ┌────────────────┐
    │ fact_virtual   │ (Sling: PostgreSQL → Databricks)
    └────────────────┘
         │
         ▼
    ┌──────────────────────────┐
    │ Databricks Job (optional) │ (Programmatic trigger)
    └──────────────────────────┘
"""

from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from databricks.sdk.service.jobs import RunResultState

import dagster as dg
from dagster import (
    AutomationCondition,
    AutomationConditionSensorDefinition,
    SensorDefinition,
    RunRequest,
    AssetSelection,
    SkipReason,
    RunsFilter,
    EnvVar,
    Field,
    AssetExecutionContext,
    build_last_update_freshness_checks,
    AssetKey,
)
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster.components import build_component_defs

from ingestion.defs.resources import DatabricksCredentials, PostgresCredentials

# Load environment variables from .env file for local development
load_dotenv()

# =============================================================================
# SENSORS
# =============================================================================

ingestion_automation_sensor = AutomationConditionSensorDefinition(
    name="default_automation_sensor",
    target=dg.AssetSelection.all(),
)
"""Default automation sensor that triggers assets based on their automation conditions.

This sensor monitors all assets with AutomationCondition.eager() and triggers
them when their upstream dependencies are updated. It runs continuously and
checks for materialization events that would satisfy eager conditions.
"""


def _is_run_in_progress(context: dg.SensorEvaluationContext) -> bool:
    """Check if there's already an ingestion run in progress.

    This prevents multiple concurrent runs of the same pipeline, which could
    cause resource contention or data consistency issues.

    Args:
        context: The sensor evaluation context with access to Dagster instance

    Returns:
        True if there's at least one run in STARTED or QUEUED status
    """
    instance = context.instance
    runs = instance.get_runs(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
        ),
        limit=1,
    )
    return len(runs) > 0


csv_ingestion_sensor = SensorDefinition(
    name="csv_ingestion_every_2_min",
    asset_selection=AssetSelection.assets(dg.AssetKey("csv_fact_virtual")),
    minimum_interval_seconds=120,
    evaluation_fn=lambda context: (
        SkipReason("An ingestion run is already in progress")
        if _is_run_in_progress(context)
        else RunRequest(partition_key=None)
    ),
)
"""Sensor that triggers csv_fact_virtual ingestion every 2 minutes.

This sensor provides an alternative to the automation sensor for periodic
execution. It includes a safeguard to skip if a run is already in progress.

Note: Currently using AutomationCondition.eager() instead, but this sensor
remains available for explicit scheduling scenarios.
"""


# =============================================================================
# ASSETS
# =============================================================================


@dg.asset(
    key="run_databricks_ingestion_job",
    group_name="ingestion",
    automation_condition=AutomationCondition.eager(),
    freshness_policy=dg.FreshnessPolicy.time_window(
        fail_window=timedelta(minutes=5),
        warn_window=timedelta(minutes=2),
    ),
    tags={"dagster/icon": "schedule", "databricks": "ingestion"},
    compute_kind="databricks",
    config_schema={
        "start_date": Field(
            str,
            default_value=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            description="Start date for Databricks job (YYYY-MM-DD format)",
        ),
        "end_date": Field(
            str,
            default_value=datetime.now().strftime("%Y-%m-%d"),
            description="End date for Databricks job (YYYY-MM-DD format)",
        ),
    },
)
def databricks_run_databricks_ingestion_job_asset(
    context: AssetExecutionContext,
    databricks: dg.ResourceParam[DatabricksCredentials],
) -> dict:
    """Trigger a Databricks notebook job with configurable date parameters.

    This asset demonstrates how to orchestrate external Databricks jobs from
    within Dagster. It submits a job run, polls for completion, and returns
    the execution results.

    Usage:
        This asset can be triggered manually with specific date ranges:
        ```
        dagster asset materialize --select run_databricks_ingestion_job \
          --config '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
        ```

    Args:
        context: Asset execution context for logging and config access
        databricks: Configured Databricks credentials resource

    Returns:
        Dictionary with run_id and status of the Databricks job

    Raises:
        TimeoutError: If the job doesn't complete within 10 minutes
        RuntimeError: If the job fails or returns an error
    """
    config = context.op_config or {}
    start_date = config.get("start_date") or (
        datetime.now() - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    end_date = config.get("end_date") or datetime.now().strftime("%Y-%m-%d")

    context.log.info(
        f"Triggering Databricks job with start_date={start_date}, end_date={end_date}"
    )

    # Skip if no Databricks credentials configured (allows testing without Databricks)
    if not databricks:
        context.log.warning("No Databricks credentials provided - skipping job trigger")
        return {"run_id": "skipped", "status": "success"}

    job_id_str = databricks.notebook_job_id
    if not job_id_str:
        context.log.warning(
            "No Databricks notebook job ID provided - skipping job trigger"
        )
        return {"run_id": "skipped", "status": "success"}

    client_wrapper = databricks.get_client()
    client = client_wrapper.workspace_client

    # Submit the job run with date parameters
    run_response = client.jobs.run_now(
        job_id=int(job_id_str),
        job_parameters={
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    run_id = run_response.run_id
    context.log.info(f"Job submitted, run_id: {run_id}")

    # Wait for job completion using SDK's built-in polling
    run = client.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=run_id,
        timeout=timedelta(seconds=600),
    )

    if run.state.result_state == RunResultState.SUCCESS:
        context.log.info(f"Job {run_id} completed successfully")

        # Try to fetch and log job output
        try:
            output_data = client.jobs.get_run_output(run_id)
            if output_data.logs:
                context.log.info(f"Job logs: {output_data.logs}")
            if output_data.notebook_output:
                result = output_data.notebook_output.result
                if result:
                    context.log.info(f"Notebook result: {result}")
        except Exception as e:
            context.log.warning(f"Could not fetch job output: {e}")

        return {"run_id": run_id, "status": "success"}
    else:
        raise RuntimeError(
            f"Job {run_id} failed: {run.state.state_message or 'Unknown error'}"
        )


# =============================================================================
# FRESHNESS CHECKS
# =============================================================================

# Freshness checks monitor when assets were last materialized and raise
# alerts if they exceed the expected update frequency.

csv_fact_virtual_freshness_checks = build_last_update_freshness_checks(
    assets=[AssetKey("csv_fact_virtual")],
    lower_bound_delta=timedelta(minutes=1),
    severity=dg.AssetCheckSeverity.ERROR,
)
"""Freshness check for csv_fact_virtual asset.

Fails if the asset hasn't been updated in the last 1 minute.
This is aggressive for demonstration - in production, use longer windows
based on your actual data refresh SLAs.
"""

fact_virtual_freshness_checks = build_last_update_freshness_checks(
    assets=[AssetKey("fact_virtual")],
    lower_bound_delta=timedelta(minutes=1),
    severity=dg.AssetCheckSeverity.ERROR,
)
"""Freshness check for fact_virtual asset.

Note: This check is on the Sling-managed asset. The freshness policy
configured in the component provides additional monitoring with warn/fail
thresholds.
"""

# Combine all freshness checks for registration
all_freshness_checks = list(csv_fact_virtual_freshness_checks) + list(
    fact_virtual_freshness_checks
)


# =============================================================================
# DEFINITIONS
# =============================================================================


def get_definitions() -> dg.Definitions:
    """Build and return the complete Dagster Definitions object.

    This function assembles all components of the ingestion pipeline:
    1. Component-based Sling assets (from YAML in components/)
    2. Programmatic Databricks orchestration asset
    3. Resources for database connections
    4. Sensors for automation and freshness monitoring
    5. Freshness checks for data quality

    Returns:
        Definitions object containing all assets, resources, and sensors
    """
    # Configure Databricks resource from environment variables
    databricks_resource = DatabricksCredentials(
        host=EnvVar("DATABRICKS_HOST").get_value() or "",
        token=EnvVar("DATABRICKS_TOKEN").get_value() or "",
        warehouse_id=EnvVar("DATABRICKS_WAREHOUSE_ID").get_value(),
        http_path=EnvVar("DATABRICKS_HTTP_PATH").get_value(),
        catalog=EnvVar("DATABRICKS_CATALOG").get_value() or "test",
        schema_name=EnvVar("DATABRICKS_SCHEMA").get_value() or "main",
        notebook_job_id=EnvVar("DATABRICKS_NOTEBOOK_JOB_ID").get_value(),
    )

    # Build component definitions from the components/ directory
    # This discovers and loads all YAML-based components (Sling replications)
    component_defs = build_component_defs(Path(__file__).parent / "components")

    # Create freshness sensor for visibility in lineage
    freshness_sensor = dg.build_sensor_for_freshness_checks(
        name="freshness_sensor",
        minimum_interval_seconds=30,
        freshness_checks=all_freshness_checks,
    )

    # Merge component definitions with programmatic definitions and source assets
    merged_defs = dg.Definitions.merge(
        component_defs,
        dg.Definitions(
            assets=[
                databricks_run_databricks_ingestion_job_asset,
            ],
            asset_checks=all_freshness_checks,
            resources={
                "databricks": databricks_resource,
                "postgres": PostgresCredentials(),
            },
            sensors=[
                csv_ingestion_sensor,
                ingestion_automation_sensor,
                freshness_sensor,
            ],
        ),
    )

    return dg.Definitions(
        assets=merged_defs.assets,
        resources=merged_defs.resources,
        sensors=merged_defs.sensors,
        asset_checks=merged_defs.asset_checks,
        jobs=merged_defs.jobs,
        schedules=merged_defs.schedules,
    )


# The global definitions object loaded by Dagster
defs = get_definitions()


def ingestion_defs() -> dg.Definitions:
    """Wrapper function for tools that expect a function attribute.

    This allows the workspace.yaml to reference either:
    - ingestion.definitions:defs (the object directly)
    - ingestion.definitions:ingestion_defs (via this function)
    """
    return defs
