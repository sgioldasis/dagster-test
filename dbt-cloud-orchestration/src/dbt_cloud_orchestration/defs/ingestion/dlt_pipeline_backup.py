# src/dbt_cloud_orchestration/defs/ingestion/dlt_pipeline.py

import os
import dlt
import pandas as pd
import dagster as dg
from dagster import (
    asset,
    Definitions,
    ScheduleDefinition,
    FreshnessPolicy,
    AssetKey,
)
from dagster_dlt import dlt_assets, DagsterDltResource
from datetime import timedelta
import shutil
import uuid


# Configure DLT pipeline for Databricks
def configure_dlt_pipeline():
    """Configure DLT pipeline to load data into Databricks"""

    # Get Databricks connection details from environment variables
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")

    # Default to test.main as requested, overriding any volume path inference
    databricks_catalog = os.getenv("DATABRICKS_CATALOG", "test")
    databricks_schema = os.getenv("DATABRICKS_SCHEMA", "main")

    # Inferred http_path from warehouse_id if missing
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    if not http_path and warehouse_id:
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    # Generate a unique pipeline name to avoid storage conflicts
    pipeline_name = f"databricks_ingestion_{uuid.uuid4().hex[:8]}"

    # Configure DLT pipeline with dev_mode=True to avoid persistent storage issues
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.databricks(
            credentials={
                "server_hostname": databricks_host,
                "access_token": databricks_token,
                "http_path": http_path,
                "catalog": databricks_catalog,
            },
            dataset_name=databricks_schema,
        ),
        dev_mode=True,  # Use dev_mode to avoid persistent storage issues
    )

    return pipeline


@dlt.resource(name="customers", write_disposition="replace")
def customers_resource():
    """Load customers data from CSV file"""
    data_path = os.getenv("CUSTOMERS_DATA_PATH", "data/raw_customers.csv")

    try:
        df = pd.read_csv(data_path)
        yield from df.to_dict(orient="records")
    except FileNotFoundError:
        print(f"Warning: Could not find customers data at {data_path}")
        yield from []


@dlt.resource(name="orders", write_disposition="replace")
def orders_resource():
    """Load orders data from CSV file"""
    data_path = os.getenv("ORDERS_DATA_PATH", "data/raw_orders.csv")

    try:
        df = pd.read_csv(data_path)
        yield from df.to_dict(orient="records")
    except FileNotFoundError:
        print(f"Warning: Could not find orders data at {data_path}")
        yield from []


@dlt.resource(name="payments", write_disposition="replace")
def payments_resource():
    """Load payments data from CSV file"""
    data_path = os.getenv("PAYMENTS_DATA_PATH", "data/raw_payments.csv")

    try:
        df = pd.read_csv(data_path)
        yield from df.to_dict(orient="records")
    except FileNotFoundError:
        print(f"Warning: Could not find payments data at {data_path}")
        yield from []


@dlt.resource(name="fact_virtual", write_disposition="replace")
def fact_virtual_resource():
    """Load fact_virtual data from CSV file"""
    data_path = os.getenv("FACT_VIRTUAL_DATA_PATH", "data/raw_fact_virtual.csv")

    try:
        df = pd.read_csv(data_path)
        yield from df.to_dict(orient="records")
    except FileNotFoundError:
        print(
            f"Warning: Could not find fact_virtual data at {data_path}. Generating test data to ensure table creation."
        )
        # Generate synthetic test data to ensuring table creation
        yield [
            {
                "id": 1,
                "virtual_item": "sword",
                "cost": 100,
                "timestamp": "2023-01-01T10:00:00Z",
            },
            {
                "id": 2,
                "virtual_item": "shield",
                "cost": 150,
                "timestamp": "2023-01-01T11:00:00Z",
            },
            {
                "id": 3,
                "virtual_item": "potion",
                "cost": 50,
                "timestamp": "2023-01-01T12:00:00Z",
            },
            {
                "id": 4,
                "virtual_item": "helmet",
                "cost": 200,
                "timestamp": "2023-01-01T13:00:00Z",
            },
            {
                "id": 5,
                "virtual_item": "boots",
                "cost": 75,
                "timestamp": "2023-01-01T14:00:00Z",
            },
        ]


@dlt.source
def csv_data_source():
    """DLT source for CSV data ingestion"""
    yield customers_resource
    yield orders_resource
    yield payments_resource


@dlt.source(name="kaizen_wars")
def kaizen_wars_source():
    """DLT source for Kaizen Wars data ingestion"""
    yield fact_virtual_resource


# Create Dagster assets for DLT pipeline
@dlt_assets(
    dlt_source=csv_data_source(),
    dlt_pipeline=configure_dlt_pipeline(),
    group_name="ingestion",
)
def dlt_databricks_assets(context, dlt: DagsterDltResource):
    """Dagster assets for DLT Databricks ingestion"""
    yield from dlt.run(
        context=context,
        dlt_source=csv_data_source(),
        dlt_pipeline=configure_dlt_pipeline(),
    )


@dlt_assets(
    dlt_source=kaizen_wars_source(),
    dlt_pipeline=configure_dlt_pipeline(),
    group_name="ingestion",
)
def kaizen_wars_ingest_assets(context, dlt: DagsterDltResource):
    """Dagster assets for Kaizen Wars DLT ingestion"""
    yield from dlt.run(
        context=context,
        dlt_source=kaizen_wars_source(),
        dlt_pipeline=configure_dlt_pipeline(),
    )


# Create individual assets with specific freshness policies


# Apply freshness policies to both DLT assets with correct asset keys
# Keep freshness policy for UI but disable automation to prevent automatic runs
dlt_databricks_assets = dlt_databricks_assets.map_asset_specs(
    lambda spec: spec._replace(
        automation_condition=None,  # Disable automatic runs
        freshness_policy=FreshnessPolicy.cron(
            deadline_cron="*/1 * * * *",  # Check freshness every minute
            lower_bound_delta=timedelta(minutes=1),
        ),
    )
)

kaizen_wars_ingest_assets = kaizen_wars_ingest_assets.map_asset_specs(
    lambda spec: spec._replace(
        automation_condition=None,  # Disable automatic runs
        freshness_policy=FreshnessPolicy.cron(
            deadline_cron="*/1 * * * *",  # Check freshness every minute
            lower_bound_delta=timedelta(minutes=1),
        ),
    )
)

# Include all assets in definitions
all_assets = [dlt_databricks_assets, kaizen_wars_ingest_assets]


# Schedule for Kaizen Wars DLT ingestion (Every 5 minutes)
kaizen_wars_dlt_schedule = ScheduleDefinition(
    name="kaizen_wars_dlt_schedule",
    target=dg.AssetSelection.keys("dlt_kaizen_wars_fact_virtual"),
    cron_schedule="*/5 * * * *",
)


def get_dlt_definitions():
    """Get Dagster definitions for DLT assets"""
    return Definitions(
        assets=all_assets,
        schedules=[kaizen_wars_dlt_schedule],
    )
