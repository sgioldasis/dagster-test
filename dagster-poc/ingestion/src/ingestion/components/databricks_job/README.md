# Databricks Job Component
#
# This component triggers external Databricks notebook jobs from Dagster.
# It submits a job run with configurable parameters, polls for completion,
# and returns results.
#
# Configuration:
#   - spec: Asset spec for the job asset (key, group_name, automation_condition, etc.)
#   - job_id: Databricks job ID to trigger
#   - host: Databricks workspace host
#   - token: Personal access token for authentication
#   - job_parameters: Static dictionary of parameters passed to the Databricks job
#   - timeout_seconds: Maximum wait time for job completion (default: 600)
#
# Partition Configuration (for scheduled/recurring runs):
#   - partition_start_date: Start date for daily partitions (YYYY-MM-DD)
#   - partition_end_date: End date for daily partitions (optional)
#   - partition_end_offset: Number of days beyond current time (default: 1)
#
# Runtime Configuration (UI-configurable at launch):
#   - start_date: Start date for the ingestion job (YYYY-MM-DD), defaults to yesterday
#   - end_date: End date for the ingestion job (YYYY-MM-DD), defaults to today
#
# Required Environment Variables:
#   - DATABRICKS_HOST: Databricks workspace host (e.g., adb-xxx.azuredatabricks.net)
#   - DATABRICKS_TOKEN: Personal access token
#
# Job-specific environment variables:
#   - DATABRICKS_NOTEBOOK_JOB_ID: Job ID for ingestion job
#   - DATABRICKS_TRANSFORM_JOB_ID: Job ID for transform job
#
# Jobs are defined in subdirectories:
#   - ingestion_job/defs.yaml - Data ingestion job
#   - transform_job/defs.yaml - Data transformation job
#
# To add a new job, create a new subdirectory with a defs.yaml file:
#   mkdir my_new_job
#   # create my_new_job/defs.yaml with your configuration
