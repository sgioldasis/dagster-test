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

from dagster import EnvVar


@dlt.resource(name="dlt_fact_virtual", write_disposition="replace")
def raw_fact_virtual_csv() -> Iterator[TDataItem]:
    """Load raw fact_virtual data from CSV file.

    This resource reads the raw_fact_virtual.csv file and yields records
    for loading into PostgreSQL.

    Yields:
        Dictionary records from the CSV file.
    """
    csv_data_path = EnvVar("CSV_DATA_PATH").get_value()
    if csv_data_path is None:
        raise ValueError("CSV_DATA_PATH environment variable is not set")
    csv_path = Path(csv_data_path) / "raw_fact_virtual.csv"
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

    Note:
        This resource uses the postgres connection defined in the pipeline.
    """
    # The source will be configured with postgres credentials at runtime
    # This is a placeholder that will be used by dlt's sql_table source
    yield from []
