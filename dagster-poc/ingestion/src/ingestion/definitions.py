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
from datetime import timedelta
from pathlib import Path

# Dagster
import dagster as dg
from dagster import (
    AssetKey,
    AutomationConditionSensorDefinition,
    build_last_update_freshness_checks,
)
from dagster.components import build_component_defs

# Local imports
from ingestion.defs.resources import PostgresCredentials


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


def _build_resources() -> dict[str, dg.ResourceDefinition]:
    """Build all resources for the ingestion pipeline.

    Uses settings from config.py for validation and defaults.

    Returns:
        Dictionary of resource definitions.
    """
    return {
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
