"""Asset definitions for dbt Cloud orchestration."""

from __future__ import annotations

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
    AssetsDefinition,
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
from .constants import (
    DbtCloudRunStatus,
    DEFAULT_KAIZEN_WARS_TARGET_KEY,
    DEFAULT_KAIZEN_WARS_PACKAGE,
    DEFAULT_FRESHNESS_DEADLINE_CRON,
    DEFAULT_FRESHNESS_LOWER_BOUND_MINUTES,
    DEFAULT_MAX_POLLING_ITERATIONS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DELAY_SECONDS,
)
from .types import DbtCloudDefinitions


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


def _trigger_job_with_retry(
    url: str,
    headers: dict[str, str],
    max_retries: int,
    retry_delay: int,
    context: dg.OpExecutionContext,
) -> dict[str, Any]:
    """Trigger a dbt Cloud job with retry logic."""
    for attempt in range(max_retries):
        try:
            response = requests.post(
                url, headers=headers, json={"cause": "Triggered from Dagster"}, timeout=30
            )
            response.raise_for_status()
            return response.json()["data"]
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                context.log.warning(
                    f"API call failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying..."
                )
                time.sleep(retry_delay)
            else:
                context.log.error(f"API call failed after {max_retries} attempts: {e}")
                raise


def _poll_run_status(
    status_url: str,
    headers: dict[str, str],
    polling_interval: int,
    max_iterations: int,
    context: dg.OpExecutionContext,
) -> dict[str, Any]:
    """Poll dbt Cloud run status until completion or timeout."""
    for iteration in range(max_iterations):
        try:
            status_resp = requests.get(status_url, headers=headers, timeout=30)
            status_resp.raise_for_status()
            
            run_data = status_resp.json()["data"]
            status = run_data["status"]
            
            if DbtCloudRunStatus.is_running(status):
                context.log.info(
                    f"Job still running, status: {status} ({DbtCloudRunStatus(status).name})"
                )
                time.sleep(polling_interval)
                continue
            
            if DbtCloudRunStatus.is_terminal(status):
                context.log.info(
                    f"Job completed with status: {status} ({DbtCloudRunStatus(status).name})"
                )
                return run_data
            
            context.log.warning(f"Unknown status: {status}")
            time.sleep(polling_interval)
            
        except requests.exceptions.RequestException as e:
            context.log.warning(f"Polling error: {e}. Retrying...")
            time.sleep(polling_interval)
    
    raise TimeoutError(
        f"Job polling timed out after {max_iterations} iterations "
        f"(~{max_iterations * polling_interval / 60:.1f} minutes)"
    )


def _create_dbt_cloud_job_trigger_job(
    credentials: DbtCloudCredentials,
    run_config: Optional[DbtCloudRunConfig] = None,
) -> Optional[JobDefinition]:
    """Create a job for triggering dbt Cloud runs, or None if job_id not configured."""
    if not credentials.job_id:
        return None

    timeout = (
        run_config.timeout_seconds if run_config else credentials.run_timeout_seconds
    )
    max_retries = run_config.max_retries if run_config else DEFAULT_MAX_RETRIES
    retry_delay = run_config.retry_delay_seconds if run_config else DEFAULT_RETRY_DELAY_SECONDS
    polling_interval = run_config.polling_interval_seconds if run_config else 30
    max_iterations = min(DEFAULT_MAX_POLLING_ITERATIONS, timeout // polling_interval)

    @op
    def trigger_dbt_cloud_job_op(context) -> dict[str, Any]:
        """Trigger a specific dbt Cloud job directly via API."""
        job_id = credentials.job_id
        account_id = str(credentials.account_id)
        token = credentials.token
        access_url = credentials.access_url

        context.log.info(f"Triggering dbt Cloud job {job_id}")

        url = f"{access_url}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
        headers = {"Authorization": f"Bearer {token}"}

        # Trigger job with retry logic
        run_data = _trigger_job_with_retry(
            url, headers, max_retries, retry_delay, context
        )
        run_id = run_data["id"]
        context.log.info(f"Job triggered, run ID: {run_id}")

        # Poll for completion
        status_url = f"{access_url}/api/v2/accounts/{account_id}/runs/{run_id}/"
        run_data = _poll_run_status(
            status_url, headers, polling_interval, max_iterations, context
        )

        status = run_data["status"]
        return {"run_id": run_id, "status": status, "run_data": run_data}

    @job(name="dbt_cloud_job_trigger")
    def dbt_cloud_job_trigger():
        trigger_dbt_cloud_job_op()

    return dbt_cloud_job_trigger


def create_dbt_cloud_definitions(
    credentials: DbtCloudCredentials,
    run_config: Optional[DbtCloudRunConfig] = None,
) -> DbtCloudDefinitions:
    """Create dbt Cloud workspace, assets, sensor, and job trigger from credentials."""
    workspace = create_dbt_cloud_workspace(credentials)
    dbt_cloud_job_trigger = _create_dbt_cloud_job_trigger_job(credentials, run_config)

    timeout = (
        run_config.timeout_seconds if run_config else credentials.run_timeout_seconds
    )

    # Use custom translator to avoid asset key collisions when multiple dbt packages
    # have models with the same name (e.g., bonus_cost in dbt_optimove and dbt_rewards)
    custom_translator = PackageAwareDbtTranslator()

    @dbt_cloud_assets(
        workspace=workspace,
        dagster_dbt_translator=custom_translator,
    )
    def my_dbt_cloud_assets(
        context, dbt_cloud: DbtCloudWorkspace
    ):
        yield from dbt_cloud.cli(args=["build"], context=context).wait(timeout=timeout)

    # Apply group_name to all specs including upstream dependencies
    my_dbt_cloud_assets = my_dbt_cloud_assets.map_asset_specs(
        lambda spec: spec.replace_attributes(group_name="dbt")
    )

    discovered_asset_keys = {k.path[-1] for k in my_dbt_cloud_assets.keys}
    target_key = DEFAULT_KAIZEN_WARS_TARGET_KEY
    is_discovered = target_key in discovered_asset_keys

    kaizen_wars_assets: list[AssetsDefinition] = []

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
                        deadline_cron=DEFAULT_FRESHNESS_DEADLINE_CRON,
                        lower_bound_delta=timedelta(minutes=DEFAULT_FRESHNESS_LOWER_BOUND_MINUTES),
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
            freshness_policy=FreshnessPolicy.cron(
                deadline_cron=DEFAULT_FRESHNESS_DEADLINE_CRON,
                lower_bound_delta=timedelta(minutes=DEFAULT_FRESHNESS_LOWER_BOUND_MINUTES),
            ),
            compute_kind="dbt",
            group_name="dbt",
        )
        def stg_kaizen_wars__fact_virtual(
            context, dbt_cloud: DbtCloudWorkspace
        ):
            """Formally managed dbt asset for Kaizen Wars fact_virtual (Explicit Definition)."""
            yield from dbt_cloud.cli(
                args=["build", "--select", target_key], context=context
            ).wait(timeout=timeout)

        kaizen_wars_assets = [stg_kaizen_wars__fact_virtual]

    dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)

    return DbtCloudDefinitions(
        assets=my_dbt_cloud_assets,
        sensor=dbt_cloud_polling_sensor,
        workspace=workspace,
        job_trigger=dbt_cloud_job_trigger,
        additional_assets=kaizen_wars_assets,
    )


__all__ = [
    "PackageAwareDbtTranslator",
    "create_dbt_cloud_definitions",
    "create_dbt_cloud_workspace",
]
