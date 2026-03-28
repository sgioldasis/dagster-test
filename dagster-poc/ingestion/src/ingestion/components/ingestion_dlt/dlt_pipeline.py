"""DLT resources for ingestion pipeline.

This module defines dlt resources and sources for loading data from:
- CSV files to PostgreSQL (dlt_csv_fact_virtual)
- PostgreSQL to Databricks (dlt_databricks_fact_virtual)

The resources use the dlt library's decorator pattern to define extractable data sources.
"""

from pathlib import Path
from typing import Iterator

import dlt
import pandas as pd
from dlt.common.typing import TDataItem
from dlt.sources.sql_database import sql_table

from ingestion.config import get_settings


@dlt.resource(name="dlt_fact_virtual", write_disposition="replace")
def raw_fact_virtual_csv() -> Iterator[TDataItem]:
    """Load raw fact_virtual data from CSV file.

    This resource reads the raw_fact_virtual.csv file and yields records
    for loading into PostgreSQL.

    Yields:
        Dictionary records from the CSV file.
    """
    settings = get_settings()
    csv_path = settings.csv_data_path / "raw_fact_virtual.csv"
    
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}. "
            f"Ensure CSV_DATA_PATH is set correctly (current: {settings.csv_data_path})"
        )
    
    df = pd.read_csv(csv_path)
    yield from df.to_dict(orient="records")


@dlt.source
def csv_source():
    """DLT source for CSV file ingestion.

    This source provides resources for loading data from CSV files.
    """
    yield raw_fact_virtual_csv


@dlt.resource(name="dlt_fact_virtual", write_disposition="replace")
def fact_virtual_postgres() -> Iterator[TDataItem]:
    """Load fact_virtual data from PostgreSQL.

    This resource reads data from the public.fact_virtual table in PostgreSQL
    and yields records for loading into Databricks.

    Yields:
        Dictionary records from the PostgreSQL table.

    Raises:
        ConnectionError: If unable to connect to PostgreSQL.
    """
    settings = get_settings()
    
    # Use dlt's sql_table source to read from PostgreSQL
    table = sql_table(
        credentials=settings.postgres_connection_string,
        schema="public",
        table="fact_virtual",
    )
    
    # Yield all records from the table
    yield from table


@dlt.source
def postgres_source():
    """DLT source for PostgreSQL table ingestion.

    This source provides resources for loading data from PostgreSQL tables.
    """
    yield fact_virtual_postgres
