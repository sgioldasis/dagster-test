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

# Standard library
from datetime import datetime, timedelta
from pathlib import Path

# Third party
from databricks.sdk.service.jobs import RunResultState

# Dagster
import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    EnvVar,
    Field,
    build_last_update_freshness_checks,
)
from dagster.components import build_component_defs

# Local imports
from ingestion.config import get_settings
from ingestion.defs.resources import DatabricksCredentials, PostgresCredentials


def _build_sensors() -> list[dg.SensorDefinition]:
    """Build all sensors for the ingestion pipeline.

    Returns:
        List of sensor definitions.
    """
    automation_sensor = AutomationConditionSensorDefinition(
        name="default_automation_sensor",
        target=dg.AssetSelection.all(),
    )

    # Build freshness sensor for visibility in lineage
    freshness_sensor = dg.build_sensor_for_freshness_checks(
        name="freshness_sensor",
        minimum_interval_seconds=30,
        freshness_checks=_build_freshness_checks(),
    )

    return [automation_sensor, freshness_sensor]


def _build_schedules() -> list[dg.ScheduleDefinition]:
    """Build all schedules for the ingestion pipeline.

    Returns:
        List of schedule definitions.
    """
    # Schedule to trigger csv_fact_virtual every 2 minutes
    csv_fact_virtual_schedule = dg.ScheduleDefinition(
        name="csv_fact_virtual_schedule",
        target=dg.AssetSelection.keys("csv_fact_virtual"),
        cron_schedule="*/2 * * * *",  # Every 2 minutes
        description="Trigger csv_fact_virtual asset every 2 minutes",
    )

    return [csv_fact_virtual_schedule]


def _build_freshness_checks() -> list:
    """Build freshness checks for monitoring data quality.

    Returns:
        List of freshness check definitions.
    """
    csv_freshness = build_last_update_freshness_checks(
        assets=[AssetKey("csv_fact_virtual")],
        lower_bound_delta=timedelta(minutes=1),
        severity=dg.AssetCheckSeverity.ERROR,
    )

    fact_freshness = build_last_update_freshness_checks(
        assets=[AssetKey("fact_virtual")],
        lower_bound_delta=timedelta(minutes=1),
        severity=dg.AssetCheckSeverity.ERROR,
    )

    return list(csv_freshness) + list(fact_freshness)


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

    Args:
        context: Asset execution context for logging and config access
        databricks: Configured Databricks credentials resource

    Returns:
        Dictionary with run_id and status of the Databricks job
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


def _build_resources() -> dict[str, dg.ResourceDefinition]:
    """Build all resources for the ingestion pipeline.

    Uses settings from config.py for validation and defaults.

    Returns:
        Dictionary of resource definitions.
    """
    settings = get_settings()

    databricks_resource = DatabricksCredentials(
        host=EnvVar("DATABRICKS_HOST"),
        warehouse_id=settings.databricks_warehouse_id,
        http_path=settings.databricks_http_path,
        catalog=settings.databricks_catalog,
        schema_name=settings.databricks_schema,
        notebook_job_id=settings.databricks_notebook_job_id,
    )

    return {
        "databricks": databricks_resource,
        "postgres": PostgresCredentials(),
    }


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
    # Build component definitions from the components/ directory
    component_defs = build_component_defs(Path(__file__).parent / "components")

    # Build freshness checks
    freshness_checks = _build_freshness_checks()

    # Merge component definitions with programmatic definitions
    merged_defs = dg.Definitions.merge(
        component_defs,
        dg.Definitions(
            assets=[databricks_run_databricks_ingestion_job_asset],
            asset_checks=freshness_checks,
            resources=_build_resources(),
            sensors=_build_sensors(),
        ),
    )

    # Build schedules
    schedules = _build_schedules()

    return dg.Definitions(
        assets=merged_defs.assets,
        resources=merged_defs.resources,
        sensors=merged_defs.sensors,
        asset_checks=merged_defs.asset_checks,
        jobs=merged_defs.jobs,
        schedules=schedules,
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
