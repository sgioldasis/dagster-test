"""Type definitions for dbt Cloud orchestration."""

from __future__ import annotations

from typing import NamedTuple, TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace


class DbtCloudDefinitions(NamedTuple):
    """Return type for create_dbt_cloud_definitions function."""
    
    assets: dg.AssetsDefinition
    sensor: dg.SensorDefinition
    workspace: "DbtCloudWorkspace"
    job_trigger: dg.JobDefinition | None
    additional_assets: list[dg.AssetsDefinition]


class DbtCloudRunResult(NamedTuple):
    """Result from a dbt Cloud job run."""
    
    run_id: int
    status: int
    run_data: dict


__all__ = ["DbtCloudDefinitions", "DbtCloudRunResult"]
