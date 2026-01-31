# dbt/src/dbt/defs/assets.py
import time
from typing import Optional, Mapping, Any
from datetime import timedelta

import dagster as dg
from dagster import (
    job,
    op,
    asset,
    FreshnessPolicy,
    JobDefinition,
    AssetKey,
)
from dagster_dbt.cloud_v2.asset_decorator import dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import (
    DbtCloudCredentials as DbtCloudSdkCredentials,
    DbtCloudWorkspace,
)
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator
import requests

from .resources import DbtCloudCredentials, DbtCloudRunConfig


class PackageAwareDbtTranslator(DagsterDbtTranslator):
    """Custom translator that includes dbt package name in asset keys to avoid collisions.
    
    When multiple dbt packages have models with the same name (e.g., bonus_cost in both
    dbt_optimove and dbt_rewards), the default translator generates identical asset keys.
    This translator prefixes asset keys with the dbt package name to ensure uniqueness.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """Generate asset key with package name prefix to avoid collisions.
        
        Args:
            dbt_resource_props: Dictionary containing dbt resource properties.
                See: https://docs.getdbt.com/reference/artifacts/manifest-json#resource-details
        
        Returns:
            AssetKey with package name as prefix (e.g., ["dbt_optimove", "bonus_cost"]).
        """
        # Get the default asset key from the parent class
        base_key = super().get_asset_key(dbt_resource_props)
        
        # Get the package name from the dbt resource properties
        # The package_name field is available for all dbt resources
        package_name = dbt_resource_props.get("package_name")
        
        # If we have a package name, prefix the asset key with it
        if package_name:
            return AssetKey([package_name, *base_key.path])
        
        return base_key


def create_dbt_cloud_workspace(
    credentials: DbtCloudCredentials,
    run_config: DbtCloudRunConfig | None = None,
) -> DbtCloudWorkspace:
    """Create a DbtCloudWorkspace from DbtCloudCredentials resource."""
    sdk_credentials = DbtCloudSdkCredentials(
        account_id=credentials.account_id,
        access_url=credentials.access_url,
        token=credentials.token,
    )
    return DbtCloudWorkspace(
        credentials=sdk_credentials,
        project_id=credentials.project_id,
        environment_id=credentials.environment_id,
    )


def _create_dbt_cloud_job_trigger_job(
    credentials: DbtCloudCredentials,
    run_config: DbtCloudRunConfig | None = None,
) -> Optional[JobDefinition]:
    """Create a job for triggering dbt Cloud runs, or None if job_id not configured."""
    if not credentials.job_id:
        return None

    timeout = (
        run_config.timeout_seconds if run_config else credentials.run_timeout_seconds
    )

    @op
    def trigger_dbt_cloud_job_op(context: dg.OpExecutionContext):
        """Trigger a specific dbt Cloud job directly via API."""
        job_id = credentials.job_id
        account_id = str(credentials.account_id)
        token = credentials.token
        access_url = credentials.access_url

        context.log.info(f"Triggering dbt Cloud job {job_id}")

        url = f"{access_url}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
        headers = {"Authorization": f"Bearer {token}"}

        response = requests.post(
            url, headers=headers, json={"cause": "Triggered from Dagster"}
        )
        response.raise_for_status()

        run_data = response.json()["data"]
        run_id = run_data["id"]
        context.log.info(f"Job triggered, run ID: {run_id}")

        status_url = f"{access_url}/api/v2/accounts/{account_id}/runs/{run_id}/"
        polling_interval = run_config.polling_interval_seconds if run_config else 30

        while True:
            status_resp = requests.get(status_url, headers=headers)
            status_resp.raise_for_status()

            run_data = status_resp.json()["data"]
            status = run_data["status"]

            if status in [1, 2, 3]:
                context.log.info(f"Job still running, status: {status}")
                time.sleep(polling_interval)
                continue

            context.log.info(f"Job completed with status: {status}")
            return {"run_id": run_id, "status": status, "run_data": run_data}

    @job(name="dbt_cloud_job_trigger")
    def dbt_cloud_job_trigger():
        trigger_dbt_cloud_job_op()

    return dbt_cloud_job_trigger


def create_dbt_cloud_definitions(
    credentials: DbtCloudCredentials,
    run_config: DbtCloudRunConfig | None = None,
) -> tuple:
    """Create dbt Cloud workspace, assets, sensor, and job trigger from credentials."""
    workspace = create_dbt_cloud_workspace(credentials, run_config)
    dbt_cloud_job_trigger = _create_dbt_cloud_job_trigger_job(credentials, run_config)

    timeout = (
        run_config.timeout_seconds if run_config else credentials.run_timeout_seconds
    )

    # Use custom translator to avoid asset key collisions when multiple dbt packages
    # have models with the same name (e.g., bonus_cost in dbt_optimove and dbt_rewards)
    custom_translator = PackageAwareDbtTranslator()

    @dbt_cloud_assets(
        workspace=workspace,
        group_name="dbt",
        dagster_dbt_translator=custom_translator,
    )
    def my_dbt_cloud_assets(
        context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace
    ):
        yield from dbt_cloud.cli(args=["build"], context=context).wait(timeout=timeout)

    discovered_asset_keys = {k.path[-1] for k in my_dbt_cloud_assets.keys}
    target_key = "stg_kaizen_wars__fact_virtual"
    is_discovered = target_key in discovered_asset_keys

    kaizen_wars_assets = []

    if is_discovered:

        def map_dbt_specs(spec: dg.AssetSpec) -> dg.AssetSpec:
            spec = spec.replace_attributes(
                automation_condition=dg.AutomationCondition.any_deps_updated()
            )

            if spec.key.path[-1] == target_key:
                # Set dependency to fact_virtual from ingestion code location
                return spec.replace_attributes(
                    deps=[dg.AssetKey(["fact_virtual"])],
                    automation_condition=dg.AutomationCondition.any_deps_updated(),
                    freshness_policy=FreshnessPolicy.cron(
                        deadline_cron="*/1 * * * *",
                        lower_bound_delta=timedelta(minutes=1),
                    ),
                )
            return spec

        my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(map_dbt_specs)

    else:
        my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(
            lambda spec: spec.replace_attributes(
                automation_condition=dg.AutomationCondition.any_deps_updated()
            )
        )

        @asset(
            key=[target_key],
            deps=[dg.AssetKey(["fact_virtual"])],
            automation_condition=dg.AutomationCondition.any_deps_updated(),
            legacy_freshness_policy=dg.LegacyFreshnessPolicy(maximum_lag_minutes=1),
            compute_kind="dbt",
            group_name="dbt",
        )
        def stg_kaizen_wars__fact_virtual(
            context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace
        ):
            """Formally managed dbt asset for Kaizen Wars fact_virtual (Explicit Definition)."""
            yield from dbt_cloud.cli(
                args=["build", "--select", target_key], context=context
            ).wait(timeout=timeout)

        kaizen_wars_assets = [stg_kaizen_wars__fact_virtual]

    dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)

    return (
        my_dbt_cloud_assets,
        dbt_cloud_polling_sensor,
        workspace,
        dbt_cloud_job_trigger,
        kaizen_wars_assets,
    )


__all__ = [
    "DbtCloudCredentials",
    "DbtCloudRunConfig",
    "create_dbt_cloud_definitions",
    "workspace",
    "my_dbt_cloud_assets",
    "dbt_cloud_polling_sensor",
    "kaizen_wars_assets",
]
