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


class DatabricksJobConfig(dg.Config):
    """Configuration for Databricks job runs.
    
    These fields appear in the Dagster UI when launching the asset,
    allowing operators to specify date ranges at runtime.
    """
    start_date: str = Field(
        default_factory=lambda: (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        description="Start date for the ingestion job (YYYY-MM-DD)"
    )
    end_date: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d"),
        description="End date for the ingestion job (YYYY-MM-DD)"
    )


class DatabricksJobComponent(dg.Component, dg.Resolvable, dg.Model):
    """Component for triggering external Databricks notebook jobs.

    This component orchestrates external Databricks jobs from within Dagster.
    It submits a job run with configurable parameters, polls for completion,
    and returns execution results.

    Supports both:
    - Ad-hoc runs with UI-configurable date ranges (via DatabricksJobConfig)
    - Partitioned runs for scheduled/recurring executions with date-based partitions

    Attributes:
        spec: Asset spec defining the asset key, group, and metadata
        job_id: Databricks job ID to trigger
        host: Databricks workspace host (e.g., adb-xxx.azuredatabricks.net)
        token: Personal access token for authentication
        job_parameters: Static dictionary of parameters to pass to the job
        timeout_seconds: Maximum time to wait for job completion (default: 600)
        partition_start_date: Start date for partition range (YYYY-MM-DD)
        partition_end_date: End date for partition range (YYYY-MM-DD)
        partition_end_offset: Number of partitions to add beyond current time

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
            environment: "production"
          # Partition configuration for scheduled runs
          partition_start_date: "2024-01-01"
          partition_end_date: "2025-12-31"
        ```
    """

    spec: dg.ResolvedAssetSpec
    job_id: str | int
    host: str
    token: str
    job_parameters: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 600
    # Partition configuration
    partition_start_date: str = Field(
        default="2024-01-01",
        description="Start date for daily partitions (YYYY-MM-DD)"
    )
    partition_end_date: str | None = Field(
        default=None,
        description="End date for daily partitions (YYYY-MM-DD). If not set, uses current date + end_offset"
    )
    partition_end_offset: int = Field(
        default=1,
        description="Number of days beyond current time to include in partitions"
    )

    def _get_client(self):
        """Create a Databricks client from credentials."""
        from databricks.sdk import WorkspaceClient
        
        return WorkspaceClient(
            host=self.host,
            token=self.token,
        )

    def _get_partition_dates(self, partition_key: str | None) -> tuple[str, str]:
        """Derive start and end dates from partition key or config.
        
        For daily partitions, the partition_key is a date (YYYY-MM-DD).
        The date range is: partition_key (start) to partition_key + 1 day (end).
        
        Args:
            partition_key: The partition key from context (e.g., "2024-03-15")
            
        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        if partition_key:
            # Partition key is the start date
            start_date = partition_key
            # End date is the next day (exclusive end for typical ingestion patterns)
            start_dt = datetime.strptime(partition_key, "%Y-%m-%d")
            end_dt = start_dt + timedelta(days=1)
            end_date = end_dt.strftime("%Y-%m-%d")
            return start_date, end_date
        
        # Fallback to defaults if no partition
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        return start_date, end_date

    def execute(
        self,
        context: AssetExecutionContext,
        config: DatabricksJobConfig,
    ) -> MaterializeResult:
        """Execute the Databricks job and wait for completion.

        Args:
            context: Asset execution context for logging and config access
            config: Runtime configuration with start_date and end_date

        Returns:
            MaterializeResult with metadata about the job run

        Raises:
            RuntimeError: If the job fails or times out
        """
        job_id = str(self.job_id)
        timeout = timedelta(seconds=self.timeout_seconds)

        # Determine date range: use config for ad-hoc runs, partition for scheduled runs
        if context.has_partition_key:
            # Scheduled/partitioned run - derive dates from partition
            start_date, end_date = self._get_partition_dates(context.partition_key)
            context.log.info(f"Partitioned run for partition: {context.partition_key}")
        else:
            # Ad-hoc run - use config-provided dates
            start_date = config.start_date
            end_date = config.end_date
            context.log.info(f"Ad-hoc run with config dates")

        # Build job parameters combining static config and runtime dates
        params = dict(self.job_parameters)
        params["start_date"] = start_date
        params["end_date"] = end_date

        context.log.info(
            f"Triggering Databricks job {job_id} with parameters: {params}"
        )

        if not job_id:
            context.log.warning(
                "No Databricks notebook job ID provided - skipping job trigger"
            )
            return MaterializeResult(metadata={"run_id": "skipped", "status": "success"})

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
                    "start_date": start_date,
                    "end_date": end_date,
                    "partition_key": context.partition_key if context.has_partition_key else None,
                }
            )
        else:
            raise RuntimeError(
                f"Job {run_id} failed: {run.state.state_message or 'Unknown error'}"
            )

    def _create_partitions_def(self) -> dg.DailyPartitionsDefinition | None:
        """Create daily partitions definition if partition dates are configured."""
        try:
            end_date = self.partition_end_date or datetime.now().strftime("%Y-%m-%d")
            return dg.DailyPartitionsDefinition(
                start_date=self.partition_start_date,
                end_date=end_date,
                end_offset=self.partition_end_offset,
            )
        except Exception as e:
            # If partition configuration is invalid, log warning and return None
            print(f"Warning: Could not create partitions definition: {e}")
            return None

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions with the Databricks job asset.

        Args:
            context: Component load context.

        Returns:
            Definitions with the Databricks job asset.
        """
        spec = self.spec
        partitions_def = self._create_partitions_def()

        # Build decorator kwargs
        decorator_kwargs: dict[str, Any] = {
            "specs": [spec],
            "compute_kind": "databricks",
        }
        if partitions_def:
            decorator_kwargs["partitions_def"] = partitions_def

        @dg.multi_asset(**decorator_kwargs)
        def databricks_job_asset(
            context: AssetExecutionContext,
            config: DatabricksJobConfig,
        ) -> MaterializeResult:
            """Trigger a Databricks notebook job and wait for completion."""
            return self.execute(context=context, config=config)

        return dg.Definitions(assets=[databricks_job_asset])
