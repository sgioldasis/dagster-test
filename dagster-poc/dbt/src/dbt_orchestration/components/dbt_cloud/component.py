"""Custom Dbt Cloud orchestration component.

This component wraps dagster_dbt's DbtCloudComponent to work around a bug
where credentials are not properly converted from dict to DbtCloudCredentials.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import Any, Mapping

import dagster as dg
from dagster import AssetKey, AutomationCondition, Definitions, FreshnessPolicy
from dagster.components import Component, ComponentLoadContext
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor
from dagster_dbt.components.dbt_component_utils import DagsterDbtComponentTranslatorSettings
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator
from pydantic import Field, field_validator


class PackageAwareDbtTranslator(DagsterDbtTranslator):
    """Custom translator that includes dbt package name in asset keys to avoid collisions."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """Generate asset key with package name prefix to avoid collisions."""
        base_key = super().get_asset_key(dbt_resource_props)
        package_name = dbt_resource_props.get("package_name")
        if package_name:
            return AssetKey([package_name, *base_key.path])
        return base_key


class DbtCloudOrchestrationComponent(Component, dg.Resolvable, dg.Model):
    """Custom dbt Cloud component with credentials fix and custom translator."""

    model_config = {"arbitrary_types_allowed": True}

    workspace: DbtCloudWorkspace = Field(
        description="The dbt Cloud workspace resource to use for this component."
    )

    cli_args: list[str | dict[str, Any]] = Field(
        default_factory=lambda: ["build"],
        description="Arguments to pass to the dbt CLI when executing.",
    )

    translation_settings: DagsterDbtComponentTranslatorSettings = Field(
        default_factory=DagsterDbtComponentTranslatorSettings,
        description="Settings for translating dbt models to Dagster assets.",
    )

    target_key: str = Field(
        description="The specific dbt model key to apply freshness policy and cross-location dependencies."
    )
    freshness_deadline_cron: str = Field(
        description="Cron expression for the freshness policy deadline."
    )
    freshness_lower_bound_minutes: int = Field(
        description="Lower bound in minutes for the freshness policy."
    )
    group_name: str = Field(
        default="dbt",
        description="The group name for all dbt assets.",
    )

    @field_validator("workspace", mode="before")
    @classmethod
    def _validate_workspace(cls, v: Any) -> DbtCloudWorkspace:
        """Ensure workspace is properly constructed with typed credentials."""
        if isinstance(v, DbtCloudWorkspace):
            if hasattr(v, 'credentials') and isinstance(v.credentials, dict):
                creds = DbtCloudCredentials(**v.credentials)
                workspace_dict = dict(v)
                workspace_dict['credentials'] = creds
                return DbtCloudWorkspace(**workspace_dict)
            return v

        if isinstance(v, dict):
            workspace_dict = dict(v)
            if "credentials" in workspace_dict and isinstance(workspace_dict["credentials"], dict):
                workspace_dict["credentials"] = DbtCloudCredentials(**workspace_dict["credentials"])
            return DbtCloudWorkspace(**workspace_dict)

        return v

    @cached_property
    def translator(self) -> DagsterDbtTranslator:
        """Create translator with package-aware asset keys."""
        settings = replace(self.translation_settings, enable_code_references=False)
        return PackageAwareDbtTranslator(settings)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build definitions with custom freshness policies and dependencies."""
        workspace_data = self.workspace.fetch_workspace_data()
        manifest = workspace_data.manifest

        from dagster_dbt.asset_utils import build_dbt_specs
        from dagster_dbt.dbt_manifest import validate_manifest

        asset_specs, check_specs = build_dbt_specs(
            translator=self.translator,
            manifest=validate_manifest(manifest),
            select="*",
            exclude="",
            selector="",
            project=None,
            io_manager_key=None,
        )

        discovered_asset_keys = {k.path[-1] for k in [spec.key for spec in asset_specs]}
        is_discovered = self.target_key in discovered_asset_keys

        def map_dbt_specs(spec: dg.AssetSpec) -> dg.AssetSpec:
            if is_discovered and spec.key.path[-1] == self.target_key:
                return spec.replace_attributes(
                    deps=[AssetKey(["fact_virtual"])],
                    automation_condition=AutomationCondition.any_deps_updated(),
                    freshness_policy=FreshnessPolicy.cron(
                        deadline_cron=self.freshness_deadline_cron,
                        lower_bound_delta=timedelta(minutes=self.freshness_lower_bound_minutes),
                    ),
                    group_name=self.group_name,
                )
            return spec.replace_attributes(
                automation_condition=AutomationCondition.any_deps_updated(),
                group_name=self.group_name,
            )

        mapped_specs = [map_dbt_specs(spec) for spec in asset_specs]

        @dg.multi_asset(
            specs=mapped_specs,
            check_specs=check_specs,
            can_subset=True,
            name="dbt_cloud_assets",
        )
        def _dbt_cloud_assets(context) -> Iterator:
            invocation = self.workspace.cli(
                args=self.cli_args,
                dagster_dbt_translator=self.translator,
                context=context,
            )
            yield from invocation.wait()

        dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=self.workspace)

        fact_virtual_source = dg.SourceAsset(
            key="fact_virtual",
            description="Asset from ingestion code location",
            group_name="ingestion",
        )

        automation_sensor = dg.AutomationConditionSensorDefinition(
            name="default_automation_sensor",
            target=dg.AssetSelection.all(),
            use_user_code_server=True,
        )

        return Definitions(
            assets=[_dbt_cloud_assets, fact_virtual_source],
            sensors=[dbt_cloud_polling_sensor, automation_sensor],
            resources={"dbt_cloud": self.workspace},
        )
