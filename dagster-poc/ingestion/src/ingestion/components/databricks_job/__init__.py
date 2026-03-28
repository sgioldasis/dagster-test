"""Databricks Job Component for triggering external Databricks notebook jobs.

This module provides a custom Dagster component that orchestrates external
Databricks notebook jobs from within Dagster.

Example usage in component.yaml:
    type: ingestion.components.databricks_job.DatabricksJobComponent
    attributes:
      spec:
        key: run_databricks_ingestion_job
        group_name: ingestion
      job_id: ${ env:DATABRICKS_NOTEBOOK_JOB_ID }
      start_date: "2024-01-01"
      end_date: "2024-12-31"
"""

from .component import DatabricksJobComponent

__all__ = ["DatabricksJobComponent"]
