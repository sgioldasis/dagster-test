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
    EnvVar,
    AssetKey,
    Field,
)
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster_dlt import dlt_assets, DagsterDltResource, DagsterDltTranslator
import dlt
from typing import Iterator

from .loads import get_pipeline, get_postgres_pipeline
from .dlt_pipeline import postgres_source, csv_to_postgres_source
from .resources import DatabricksCredentials


class CsvToPostgresDltTranslator(DagsterDltTranslator):
    """Custom DLT translator for CSV → PostgreSQL pipeline."""

    def get_asset_key(self, resource) -> dg.AssetKey:
        return dg.AssetKey(["dlt_csv_to_postgres"])

    def get_group_name(self, resource) -> str:
        return "ingestion"

    def get_asset_spec(self, data) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        existing_kinds = spec.kinds or set()
        return spec.merge_attributes(kinds=existing_kinds | {"csv", "postgres"})


class KaizenWarsDltTranslator(DagsterDltTranslator):
    """Custom DLT translator for Kaizen Wars pipeline (PostgreSQL → Databricks)."""

    def get_asset_key(self, resource) -> dg.AssetKey:
        return dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])

    def get_group_name(self, resource) -> str:
        return "ingestion"

    def get_asset_spec(self, data) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        existing_kinds = spec.kinds or set()
        return spec.merge_attributes(kinds=existing_kinds | {"postgres"})


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


class DagsterDltResourceWithEnvVars(DagsterDltResource):
    """Extended DLT resource that routes to correct destination based on asset being run."""

    def __init__(self):
        super().__init__(dlt_pipeline=None)
        self._cached_databricks_pipeline = None
        self._cached_postgres_pipeline = None

    def _get_databricks_pipeline(self):
        if self._cached_databricks_pipeline is not None:
            return self._cached_databricks_pipeline

        databricks_host = EnvVar("DATABRICKS_HOST").get_value()
        databricks_token = EnvVar("DATABRICKS_TOKEN").get_value()

        if databricks_host and databricks_token:
            credentials = DatabricksCredentials(
                host=databricks_host,
                token=databricks_token,
                http_path=EnvVar("DATABRICKS_HTTP_PATH").get_value(),
                catalog=EnvVar("DATABRICKS_CATALOG").get_value() or "test",
                schema_name=EnvVar("DATABRICKS_SCHEMA").get_value() or "main",
            )
            self._cached_databricks_pipeline = get_pipeline(credentials)
            print("[DEBUG] DLT resource using Databricks pipeline")
        else:
            self._cached_databricks_pipeline = get_pipeline(None)
            print("[DEBUG] DLT resource using DuckDB pipeline")

        return self._cached_databricks_pipeline

    def _get_postgres_pipeline(self):
        if self._cached_postgres_pipeline is not None:
            return self._cached_postgres_pipeline

        self._cached_postgres_pipeline = get_postgres_pipeline()
        print("[DEBUG] DLT resource using local PostgreSQL pipeline")
        return self._cached_postgres_pipeline

    def run(
        self,
        context: dg.OpExecutionContext,
        dlt_source=None,
        dlt_pipeline=None,
        dagster_dlt_translator=None,
        **kwargs,
    ):
        asset_name = context.op.name if hasattr(context, "op") else str(context)

        if "postgres" in asset_name.lower():
            pipeline = dlt_pipeline or self._get_postgres_pipeline()
        else:
            pipeline = dlt_pipeline or self._get_databricks_pipeline()

        context.log.info(
            f"Running DLT pipeline with destination: {type(pipeline.destination).__name__}"
        )
        yield from DagsterDltResource.run(
            self,
            context=context,
            dlt_pipeline=pipeline,
            dlt_source=dlt_source,
            dagster_dlt_translator=dagster_dlt_translator,
            **kwargs,
        )


def _create_pipeline_for_decorator():
    """Create pipeline for UI metadata (uses hardcoded values since env vars load later)."""
    return dlt.pipeline(
        pipeline_name="kaizen_wars_ingestion",
        destination=dlt.destinations.databricks(
            server_hostname="<host>",
            access_token="<token>",
            http_path="/sql/1.0/warehouses/<warehouse_id>",
            catalog="test",
            schema="main",
        ),
        dataset_name="main",
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=postgres_source(),
    dlt_pipeline=_create_pipeline_for_decorator(),
    name="kaizen_wars_dlt_assets",
    dagster_dlt_translator=KaizenWarsDltTranslator(),
)
def dlt_kaizen_wars_fact_virtual_asset(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    """DLT asset for fact_virtual (PostgreSQL → Databricks)."""
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


def _create_csv_to_postgres_pipeline_for_decorator():
    """Create pipeline for UI metadata (CSV → PostgreSQL)."""
    return dlt.pipeline(
        pipeline_name="csv_to_postgres",
        destination=dlt.destinations.postgres(
            host="<localhost>",
            user="postgres",
            password="<password>",
            database="postgres",
            port=5432,
        ),
        dataset_name="public",
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=csv_to_postgres_source(),
    dlt_pipeline=_create_csv_to_postgres_pipeline_for_decorator(),
    name="csv_to_postgres_dlt_assets",
    dagster_dlt_translator=CsvToPostgresDltTranslator(),
)
def dlt_csv_to_postgres_asset(context: AssetExecutionContext, dlt: DagsterDltResource):
    """DLT asset for CSV → PostgreSQL."""
    yield from dlt.run(context=context)


dlt_csv_to_postgres_asset = apply_freshness_policies_to_dlt_assets(
    dlt_csv_to_postgres_asset
)

from datetime import datetime, timedelta
import time
import requests
import os


@dg.asset(
    key="run_databricks_ingestion_job",
    group_name="ingestion",
    automation_condition=AutomationCondition.eager(),
    tags={"dagster/icon": "schedule", "databricks": "ingestion"},
    compute_kind="databricks",
    config_schema={
        "start_date": Field(
            str,
            default_value=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            description="Start date (YYYY-MM-DD)",
        ),
        "end_date": Field(
            str,
            default_value=datetime.now().strftime("%Y-%m-%d"),
            description="End date (YYYY-MM-DD)",
        ),
    },
)
def databricks_run_databricks_ingestion_job_asset(
    context: dg.AssetExecutionContext,
):
    """Trigger a Databricks notebook job with start_date and end_date parameters."""
    config = context.op_config or {}
    start_date = config.get("start_date") or (
        datetime.now() - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    end_date = config.get("end_date") or datetime.now().strftime("%Y-%m-%d")

    context.log.info(
        f"Triggering Databricks job with start_date={start_date}, end_date={end_date}"
    )

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    job_id_str = os.environ.get("DATABRICKS_NOTEBOOK_JOB_ID")

    if not job_id_str:
        raise ValueError("DATABRICKS_NOTEBOOK_JOB_ID environment variable is not set")

    base_url = f"https://{host}/api/2.1"
    headers = {"Authorization": f"Bearer {token}"}

    run_params = {
        "job_id": int(job_id_str),
        "job_parameters": [
            {"key": "start_date", "value": start_date},
            {"key": "end_date", "value": end_date},
        ],
    }

    context.log.info(f"Request body: {run_params}")

    response = requests.post(
        f"{base_url}/jobs/run-now",
        headers=headers,
        json=run_params,
    )

    if not response.ok:
        context.log.error(
            f"Databricks API error: {response.status_code} - {response.text}"
        )
        response.raise_for_status()

    run_id = response.json()["run_id"]
    context.log.info(f"Job submitted, run_id: {run_id}")

    polling_interval = 30
    timeout_seconds = 600
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(
                f"Job {run_id} timed out after {timeout_seconds} seconds"
            )

        status_resp = requests.get(
            f"{base_url}/jobs/runs/get?run_id={run_id}",
            headers=headers,
        )
        status_resp.raise_for_status()

        run_state = status_resp.json()["state"]
        life_cycle_state = run_state.get("life_cycle_state", "PENDING")
        result_state = run_state.get("result_state", "")
        state_message = run_state.get("state_message", "")

        context.log.info(f"Job {run_id} status: {life_cycle_state}")

        if life_cycle_state in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"]:
            if result_state == "SUCCESS":
                context.log.info(f"Job {run_id} completed successfully")

                try:
                    output_resp = requests.get(
                        f"{base_url}/jobs/runs/get-output?run_id={run_id}",
                        headers=headers,
                    )
                    if output_resp.ok:
                        output_data = output_resp.json()
                        if "logs" in output_data:
                            context.log.info(f"Job logs: {output_data['logs']}")
                        if "notebook_output" in output_data:
                            notebook_output = output_data["notebook_output"]
                            if "result" in notebook_output:
                                context.log.info(
                                    f"Notebook result: {notebook_output['result']}"
                                )
                except Exception as e:
                    context.log.warning(f"Could not fetch job output: {e}")

                return {"run_id": run_id, "status": "success"}
            else:
                raise RuntimeError(f"Job {run_id} failed: {state_message}")

        time.sleep(polling_interval)


def ingestion_defs() -> dg.Definitions:
    databricks_host = EnvVar("DATABRICKS_HOST").get_value()
    databricks_token = EnvVar("DATABRICKS_TOKEN").get_value()
    databricks_catalog = EnvVar("DATABRICKS_CATALOG").get_value()
    databricks_schema = EnvVar("DATABRICKS_SCHEMA").get_value()

    print(f"[DEBUG] DATABRICKS_HOST='{databricks_host}'")
    print(f"[DEBUG] DATABRICKS_TOKEN present={bool(databricks_token)}")
    print(f"[DEBUG] DATABRICKS_CATALOG='{databricks_catalog}'")
    print(f"[DEBUG] DATABRICKS_SCHEMA='{databricks_schema}'")

    databricks_resource = None
    if databricks_host and databricks_token:
        print("[DEBUG] Creating Databricks resource")
        databricks_resource = DatabricksCredentials(
            host=databricks_host,
            token=databricks_token,
            http_path=EnvVar("DATABRICKS_HTTP_PATH").get_value(),
            catalog=databricks_catalog or "test",
            schema_name=databricks_schema or "main",
        )
    else:
        print("[DEBUG] No Databricks credentials - using DuckDB fallback")

    return Definitions(
        assets=[
            dlt_csv_to_postgres_asset,
            dlt_kaizen_wars_fact_virtual_asset,
            databricks_run_databricks_ingestion_job_asset,
        ],
        resources={
            "dlt": DagsterDltResourceWithEnvVars(),
            "databricks": databricks_resource,
        },
        sensors=[dlt_fact_virtual_sensor, ingestion_automation_sensor],
    )
