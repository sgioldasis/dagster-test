"""DBT Cloud code location using component-based configuration.

This module defines the Dagster code location for dbt Cloud orchestration.
Configuration is loaded from component.yaml and used to create definitions.
"""

from pathlib import Path
from datetime import timedelta

import dagster as dg
from dagster import (
    AutomationConditionSensorDefinition,
    SourceAsset,
    AssetKey,
    AutomationCondition,
    FreshnessPolicy,
)
from dagster_dbt.cloud_v2.asset_decorator import dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import (
    DbtCloudCredentials as DbtCloudSdkCredentials,
    DbtCloudWorkspace,
)
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator
from dotenv import load_dotenv
import yaml

# Load .env from current directory (same as original code)
load_dotenv()


class PackageAwareDbtTranslator(DagsterDbtTranslator):
    """Custom translator that includes dbt package name in asset keys to avoid collisions."""

    def get_asset_key(self, dbt_resource_props):
        base_key = super().get_asset_key(dbt_resource_props)
        package_name = dbt_resource_props.get("package_name")
        if package_name:
            return AssetKey([package_name, *base_key.path])
        return base_key


def _load_component_config() -> dict:
    """Load component configuration from YAML file."""
    config_path = Path(__file__).parent / "components" / "dbt_cloud" / "component.yaml"
    with open(config_path) as f:
        content = f.read()
    # Simple env var substitution
    import os
    import re

    def env_replacer(match):
        var_name = match.group(1).strip("'\"")
        default = match.group(2).strip("'\"") if match.group(2) else ""
        return os.environ.get(var_name, default)

    # Replace {{ env('VAR_NAME') }} with actual value
    pattern = r"\{\{\s*env\(['\"]([^'\"]+)['\"](?:,\s*['\"]([^'\"]*)['\"])?\)\s*\}\}"
    content = re.sub(pattern, env_replacer, content)
    return yaml.safe_load(content)


def _build_definitions() -> dg.Definitions:
    """Build and return the complete Dagster Definitions object."""
    # Load configuration from component.yaml
    config = _load_component_config()
    attrs = config.get("attributes", {})
    workspace_config = attrs.get("workspace", {})

    # Create workspace
    sdk_credentials = DbtCloudSdkCredentials(
        account_id=workspace_config.get("account_id", 0),
        access_url=workspace_config.get("access_url", "https://cloud.getdbt.com"),
        token=workspace_config.get("token", ""),
    )
    workspace = DbtCloudWorkspace(
        credentials=sdk_credentials,
        project_id=workspace_config.get("project_id", 0),
        environment_id=workspace_config.get("environment_id", 0),
    )

    # Get other config values
    timeout_seconds = attrs.get("timeout_seconds", 3600)
    target_key = attrs.get("target_key", "stg_kaizen_wars__fact_virtual")
    freshness_deadline_cron = attrs.get("freshness_deadline_cron", "0 6 * * *")
    freshness_lower_bound_minutes = attrs.get("freshness_lower_bound_minutes", 1440)

    # Use custom translator
    custom_translator = PackageAwareDbtTranslator()

    @dbt_cloud_assets(
        workspace=workspace,
        dagster_dbt_translator=custom_translator,
    )
    def my_dbt_cloud_assets(context, dbt_cloud: DbtCloudWorkspace):
        yield from dbt_cloud.cli(
            args=["build"], context=context
        ).wait(timeout=timeout_seconds)

    # Apply group_name
    my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(
        lambda spec: spec.replace_attributes(group_name="dbt")
    )

    # Check if target asset is discovered
    discovered_asset_keys = {k.path[-1] for k in my_dbt_cloud_assets.keys}
    is_discovered = target_key in discovered_asset_keys

    kaizen_wars_assets = []

    if is_discovered:

        def map_dbt_specs(spec):
            spec = spec.replace_attributes(
                automation_condition=AutomationCondition.any_deps_updated()
            )
            if spec.key.path[-1] == target_key:
                return spec.replace_attributes(
                    deps=[AssetKey(["fact_virtual"])],
                    automation_condition=AutomationCondition.any_deps_updated(),
                    freshness_policy=FreshnessPolicy.cron(
                        deadline_cron=freshness_deadline_cron,
                        lower_bound_delta=timedelta(minutes=freshness_lower_bound_minutes),
                    ),
                )
            return spec

        my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(map_dbt_specs)
    else:
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
                deadline_cron=freshness_deadline_cron,
                lower_bound_delta=timedelta(minutes=freshness_lower_bound_minutes),
            ),
            compute_kind="dbt",
            group_name="dbt",
        )
        def stg_kaizen_wars__fact_virtual(context, dbt_cloud: DbtCloudWorkspace):
            yield from dbt_cloud.cli(
                args=["build", "--select", target_key], context=context
            ).wait(timeout=timeout_seconds)

        kaizen_wars_assets = [stg_kaizen_wars__fact_virtual]

    # Build polling sensor
    dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)

    # Cross-location source asset
    fact_virtual_source = SourceAsset(
        key="fact_virtual",
        description="Asset from ingestion code location",
        group_name="ingestion",
    )

    # Automation sensor
    automation_sensor = AutomationConditionSensorDefinition(
        name="default_automation_sensor",
        target=dg.AssetSelection.all(),
        use_user_code_server=True,
    )

    return dg.Definitions(
        assets=[my_dbt_cloud_assets, *kaizen_wars_assets, fact_virtual_source],
        sensors=[dbt_cloud_polling_sensor, automation_sensor],
        resources={"dbt_cloud": workspace},
    )


# The global definitions object loaded by Dagster
defs = _build_definitions()


def dbt_defs() -> dg.Definitions:
    """Wrapper function for tools that expect a function attribute."""
    return defs
