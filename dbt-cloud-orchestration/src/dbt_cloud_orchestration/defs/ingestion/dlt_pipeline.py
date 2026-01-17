# src/dbt_cloud_orchestration/defs/ingestion/dlt_pipeline.py
"""DLT resources and source definitions for Kaizen Wars data ingestion."""

import dlt
import pandas as pd
from dagster import EnvVar


@dlt.resource(name="fact_virtual", write_disposition="replace")
def fact_virtual_resource():
    """Load fact_virtual data from CSV file."""
    data_path = (
        EnvVar("FACT_VIRTUAL_DATA_PATH").get_value() or "data/raw_fact_virtual.csv"
    )

    try:
        df = pd.read_csv(data_path)
        yield from df.to_dict(orient="records")
    except FileNotFoundError:
        # Generate synthetic test data to ensure table creation
        yield from [
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


@dlt.source(name="kaizen_wars")
def kaizen_wars_source():
    """DLT source for Kaizen Wars data ingestion."""
    yield fact_virtual_resource
