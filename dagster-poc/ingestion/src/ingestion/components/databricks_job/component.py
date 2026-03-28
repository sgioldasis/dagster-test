"""Databricks Job Component for triggering external Databricks notebook jobs.

This module provides a custom Dagster component that orchestrates external
Databricks notebook jobs from within Dagster.
"""

from datetime import datetime, timedelta
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext, AssetSpec, MaterializeResult
from databricks.sdk.service.jobs import RunResultState
from pydantic import Field


class DatabricksJobComponent(dg.Component, dg.Resolvable, dg.Model):
    """Component for triggering external Databricks notebook jobs.

    This component orchestrates external Databricks jobs from within Dagster.
    It submits a job run with configurable parameters, polls for completion,
    and returns execution results.

    Attributes:
        spec: Asset spec defining the asset key, group, and metadata
        job_id: Databricks job ID to trigger
        host: Databricks workspace host (e.g., adb-xxx.azuredatabricks.net)
        token: Personal access token for authentication
        job_parameters: Dictionary of parameters to pass to the job
        timeout_seconds: Maximum time to wait for job completion (default: 600)

    Example:
        ```yaml
        type: ingestion.components.databricks_job.DatabricksJobComponent
        attributes:
          spec:
            key: run_databricks_ingestion_job
            group_name: ingestion
            automation_condition: eager
          job_id: "{{ env.DATABRICKS_NOTEBOOK_JOB_ID }}"
          host: "{{ env.DATABRICKS_HOST }}"
          token: "{{ env.DATABRICKS_TOKEN }}"
          job_parameters:
            start_date: "2024-01-01"
            end_date: "2024-12-31"
        ```
    """

    spec: dg.ResolvedAssetSpec
    job_id: str | int
    host: str
    token: str
    job_parameters: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 600

    def _get_client(self):
        """Create a Databricks client from credentials."""
        from databricks.sdk import WorkspaceClient
        
        return WorkspaceClient(
            host=self.host,
            token=self.token,
        )

    def execute(
        self,
        context: AssetExecutionContext,
    ) -> MaterializeResult:
        """Execute the Databricks job and wait for completion.

        Args:
            context: Asset execution context for logging and config access

        Returns:
            MaterializeResult with metadata about the job run

        Raises:
            RuntimeError: If the job fails or times out
        """
        job_id = str(self.job_id)
        timeout = timedelta(seconds=self.timeout_seconds)

        # Build job parameters with defaults for start_date and end_date
        params = dict(self.job_parameters)
        if "start_date" not in params or not params["start_date"]:
            params["start_date"] = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if "end_date" not in params or not params["end_date"]:
            params["end_date"] = datetime.now().strftime("%Y-%m-%d")

        context.log.info(
            f"Triggering Databricks job {job_id} with parameters: {params}"
        )

        if not job_id:
            context.log.warning(
                "No Databricks notebook job ID provided - skipping job trigger"
            )
            return {"run_id": "skipped", "status": "success"}

        # Get client from credentials
        context.log.info(f"Creating Databricks client for host: {self.host}")
        client = self._get_client()

        # Submit the job run with parameters
        # Note: For notebook tasks, use notebook_params instead of job_parameters
        try:
            context.log.info(f"Calling jobs.run_now with job_id={job_id}")
            run_response = client.jobs.run_now(
                job_id=int(job_id),
                notebook_params=params,
            )
            run_id = run_response.run_id
            context.log.info(f"Job submitted successfully, run_id: {run_id}")
        except Exception as e:
            context.log.error(f"Failed to submit job: {e}")
            raise

        # Wait for job completion using SDK's built-in polling
        context.log.info(f"Waiting for job completion (timeout={timeout.seconds}s)...")
        try:
            run = client.jobs.wait_get_run_job_terminated_or_skipped(
                run_id=run_id,
                timeout=timeout,
            )
            context.log.info(f"Job finished with state: {run.state.result_state}")
        except Exception as e:
            context.log.error(f"Error waiting for job completion: {e}")
            raise

        if run.state.result_state == RunResultState.SUCCESS:
            context.log.info(f"Job {run_id} completed successfully")

            # Try to fetch and log job output
            try:
                output_data = client.jobs.get_run_output(run_id)
                if output_data.logs:
                    context.log.info(f"Job logs: {output_data.logs}")
                if output_data.notebook_output:
                    result = output_data.notebook_output.result
                    if result:
                        context.log.info(f"Notebook result: {result}")
            except Exception as e:
                context.log.warning(f"Could not fetch job output: {e}")

            return MaterializeResult(
                metadata={
                    "run_id": run_id,
                    "status": "success",
                    "job_id": job_id,
                }
            )
        else:
            raise RuntimeError(
                f"Job {run_id} failed: {run.state.state_message or 'Unknown error'}"
            )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions with the Databricks job asset.

        Args:
            context: Component load context.

        Returns:
            Definitions with the Databricks job asset.
        """
        spec = self.spec

        @dg.multi_asset(specs=[spec], compute_kind="databricks")
        def databricks_job_asset(
            context: AssetExecutionContext,
        ) -> MaterializeResult:
            """Trigger a Databricks notebook job and wait for completion."""
            return self.execute(context=context)

        return dg.Definitions(assets=[databricks_job_asset])
