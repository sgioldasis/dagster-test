"""Ingestion code location - DLT pipeline for loading data."""

import dagster as dg
from dagster import (
    Definitions,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    LegacyFreshnessPolicy,
    SensorDefinition,
    RunRequest,
    AssetSelection,
    SkipReason,
    RunsFilter,
    AssetExecutionContext,
)
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster_dlt import dlt_assets, DagsterDltResource, DagsterDltTranslator

from .loads import pipeline
from .dlt_pipeline import kaizen_wars_source


class KaizenWarsDltTranslator(DagsterDltTranslator):
    """Custom DLT translator for Kaizen Wars pipeline."""

    def get_asset_key(self, resource) -> dg.AssetKey:
        return dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])

    def get_group_name(self, resource) -> str:
        return "ingestion"


ingestion_automation_sensor = AutomationConditionSensorDefinition(
    name="default_automation_sensor",
    target=dg.AssetSelection.all(),
    use_user_code_server=True,
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


@dlt_assets(
    dlt_source=kaizen_wars_source(),
    dlt_pipeline=pipeline,
    name="kaizen_wars_dlt_assets",
    dagster_dlt_translator=KaizenWarsDltTranslator(),
)
def dlt_kaizen_wars_fact_virtual_asset(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    """DLT asset for fact_virtual."""
    yield from dlt.run(context=context)


def apply_freshness_policies_to_dlt_assets(assets_def):
    """Apply freshness policies to all assets in a DLT AssetsDefinition"""
    return assets_def.map_asset_specs(
        lambda spec: spec._replace(
            automation_condition=dg.AutomationCondition.eager(),
            legacy_freshness_policy=LegacyFreshnessPolicy(maximum_lag_minutes=1),
        )
    )


dlt_kaizen_wars_fact_virtual_asset = apply_freshness_policies_to_dlt_assets(
    dlt_kaizen_wars_fact_virtual_asset
)


def ingestion_defs() -> dg.Definitions:
    return Definitions(
        assets=[dlt_kaizen_wars_fact_virtual_asset],
        resources={"dlt": DagsterDltResource()},
        sensors=[dlt_fact_virtual_sensor, ingestion_automation_sensor],
    )
