# src/dbt_cloud_orchestration/defs/ingestion/definitions.py
"""Ingestion code location - DLT pipeline for loading data.

This code location is independent and produces the dlt_kaizen_wars_fact_virtual asset
that can be consumed by other code locations via SourceAsset references.
"""

import dagster as dg
from dagster import (
    Definitions,
    AssetSpec,
    AutomationCondition,
    LegacyFreshnessPolicy,
    SensorDefinition,
    RunRequest,
    AssetSelection,
    SkipReason,
    RunsFilter,
)
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster_dlt import dlt_assets, DagsterDltResource

from .loads import pipeline
from .dlt_pipeline import kaizen_wars_source


@dlt_assets(
    dlt_source=kaizen_wars_source(),
    dlt_pipeline=pipeline,
    name="kaizen_wars_dlt_assets",
)
def dlt_fact_virtual_asset(context, dlt: DagsterDltResource):
    """DLT asset for fact_virtual with correct key and freshness policy."""
    yield from dlt.run(context=context)


dlt_fact_virtual_asset = dlt_fact_virtual_asset.map_asset_specs(
    lambda spec: AssetSpec(
        key="dlt_kaizen_wars_fact_virtual",
        group_name="ingestion",
        description="Loads Kaizen Wars data into Databricks via DLT",
        legacy_freshness_policy=LegacyFreshnessPolicy(maximum_lag_minutes=1),
        automation_condition=AutomationCondition.any_deps_updated(),
        deps=spec.deps,
        metadata=spec.metadata,
        tags=spec.tags,
        kinds=spec.kinds,
        owners=spec.owners,
    )
)


def _is_run_in_progress(context):
    instance = context.instance
    runs = instance.get_runs(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
        ),
        limit=1,
    )
    return len(runs) > 0


dlt_fact_virtual_sensor = SensorDefinition(
    name="dlt_fact_virtual_every_2_min",
    asset_selection=AssetSelection.keys(dg.AssetKey("dlt_kaizen_wars_fact_virtual")),
    minimum_interval_seconds=120,
    evaluation_fn=lambda context: (
        SkipReason("A run is already in progress")
        if _is_run_in_progress(context)
        else RunRequest(partition_key=None)
    ),
)


ingestion_defs = Definitions(
    assets=[dlt_fact_virtual_asset],
    resources={"dlt": DagsterDltResource()},
    sensors=[dlt_fact_virtual_sensor],
)
