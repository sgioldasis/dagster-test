"""Dbt Cloud orchestration component.

This component provides declarative configuration for dbt Cloud orchestration,
replacing the imperative Python code in defs/assets.py.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional
from datetime import timedelta
from pathlib import Path

import dagster as dg
from dagster import (
    AssetKey,
    AssetSpec,
    FreshnessPolicy,
    Definitions,
    AutomationCondition,
)
from dagster.components import Component, ComponentLoadContext
from dagster_dbt.cloud_v2.asset_decorator import dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import (
    DbtCloudCredentials as DbtCloudSdkCredentials,
    DbtCloudWorkspace,
)
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator


class PackageAwareDbtTranslator(DagsterDbtTranslator):
    """Custom translator that includes dbt package name in asset keys to avoid collisions.

    When multiple dbt packages have models with the same name (e.g., bonus_cost in both
    dbt_optimove and dbt_rewards), the default translator generates identical asset keys.
    This translator prefixes asset keys with the dbt package name to ensure uniqueness.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """Generate asset key with package name prefix to avoid collisions."""
        # Get the default asset key from the parent class
        base_key = super().get_asset_key(dbt_resource_props)

        # Get the package name from the dbt resource properties
        package_name = dbt_resource_props.get("package_name")

        # If we have a package name, prefix the asset key with it
        if package_name:
            return AssetKey([package_name, *base_key.path])

        return base_key


class DbtCloudOrchestrationComponent(Component):
    """Component for orchestrating dbt Cloud jobs.

    This component creates:
    - dbt Cloud assets with package-aware asset keys
    - Polling sensor for dbt Cloud job status
    - Optional additional assets for cross-location dependencies

    Example component.yaml:
        type: dbt_orchestration.components.dbt_cloud.DbtCloudOrchestrationComponent
        attributes:
          workspace:
            account_id: 12345
            project_id: 67890
            environment_id: 11111
            token: "{{ env('DBT_CLOUD_TOKEN') }}"
          timeout_seconds: 3600
    """

    # Component configuration attributes - these are resolved from YAML
    workspace: dict
    timeout_seconds: int = 3600
    target_key: str = "stg_kaizen_wars__fact_virtual"
    freshness_deadline_cron: str = "0 6 * * *"
    freshness_lower_bound_minutes: int = 1440

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from component configuration."""
        # Create workspace
        sdk_credentials = DbtCloudSdkCredentials(
            account_id=self.workspace.get("account_id", 0),
            access_url=self.workspace.get("access_url", "https://cloud.getdbt.com"),
            token=self.workspace.get("token", ""),
        )
        workspace = DbtCloudWorkspace(
            credentials=sdk_credentials,
            project_id=self.workspace.get("project_id", 0),
            environment_id=self.workspace.get("environment_id", 0),
        )

        # Use custom translator to avoid asset key collisions
        custom_translator = PackageAwareDbtTranslator()

        @dbt_cloud_assets(
            workspace=workspace,
            dagster_dbt_translator=custom_translator,
        )
        def my_dbt_cloud_assets(
            context, dbt_cloud: DbtCloudWorkspace
        ):
            yield from dbt_cloud.cli(
                args=["build"], context=context
            ).wait(timeout=self.timeout_seconds)

        # Apply group_name to all specs
        my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(
            lambda spec: spec.replace_attributes(group_name="dbt")
        )

        # Check if target asset is discovered
        discovered_asset_keys = {k.path[-1] for k in my_dbt_cloud_assets.keys}
        target_key = self.target_key
        is_discovered = target_key in discovered_asset_keys

        kaizen_wars_assets: list[dg.AssetsDefinition] = []

        if is_discovered:
            # Target asset is in dbt Cloud - configure it with deps and freshness
            def map_dbt_specs(spec: AssetSpec) -> AssetSpec:
                spec = spec.replace_attributes(
                    automation_condition=AutomationCondition.any_deps_updated()
                )

                if spec.key.path[-1] == target_key:
                    return spec.replace_attributes(
                        deps=[AssetKey(["fact_virtual"])],
                        automation_condition=AutomationCondition.any_deps_updated(),
                        freshness_policy=FreshnessPolicy.cron(
                            deadline_cron=self.freshness_deadline_cron,
                            lower_bound_delta=timedelta(
                                minutes=self.freshness_lower_bound_minutes
                            ),
                        ),
                    )
                return spec

            my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(map_dbt_specs)
        else:
            # Target asset not in dbt Cloud - create it explicitly
            my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(
                lambda spec: spec.replace_attributes(
                    automation_condition=AutomationCondition.any_deps_updated()
                )
            )

            @dg.asset(
                key=[target_key],
                deps=[AssetKey(["fact_virtual"])],
                automation_condition=AutomationCondition.any_deps_updated(),
                freshness_policy=FreshnessPolicy.cron(
                    deadline_cron=self.freshness_deadline_cron,
                    lower_bound_delta=timedelta(
                        minutes=self.freshness_lower_bound_minutes
                    ),
                ),
                compute_kind="dbt",
                group_name="dbt",
            )
            def stg_kaizen_wars__fact_virtual(
                context, dbt_cloud: DbtCloudWorkspace
            ):
                """Formally managed dbt asset for Kaizen Wars fact_virtual."""
                yield from dbt_cloud.cli(
                    args=["build", "--select", target_key], context=context
                ).wait(timeout=self.timeout_seconds)

            kaizen_wars_assets = [stg_kaizen_wars__fact_virtual]

        # Build polling sensor
        dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)

        return Definitions(
            assets=[my_dbt_cloud_assets, *kaizen_wars_assets],
            sensors=[dbt_cloud_polling_sensor],
            resources={"dbt_cloud": workspace},
        )
