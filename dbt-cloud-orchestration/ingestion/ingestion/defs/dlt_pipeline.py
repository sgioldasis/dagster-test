# ingestion/src/ingestion/defs/dlt_pipeline.py
"""DLT resources and source definitions for Kaizen Wars data ingestion."""

import dlt
import pandas as pd
from dagster import EnvVar
from dlt.sources.sql_database import sql_database


from .utils import get_postgres_connection_string


@dlt.resource(
    name="fact_virtual",
    write_disposition="replace",
)
def fact_virtual_csv_resource():
    """Load fact_virtual data from CSV file."""
    from pathlib import Path

    data_path = EnvVar("FACT_VIRTUAL_DATA_PATH").get_value()
    if not data_path:
        # Default to monorepo root data folder
        data_path = str(
            Path(__file__).parent.parent.parent.parent / "data" / "raw_fact_virtual.csv"
        )

    try:
        df = pd.read_csv(data_path)
        yield from df.to_dict(orient="records")
    except FileNotFoundError as e:
        raise FileNotFoundError(f"CSV file not found at: {data_path}") from e


@dlt.source(name="postgres")
def postgres_source():
    """DLT source for PostgreSQL using the native sql_database source."""
    conn_string = get_postgres_connection_string()
    
    # Be explicit about the table name to ensure it's found
    source = sql_database(conn_string, table_names=["fact_virtual"])
    
    # FORCE 'replace' mode
    if "fact_virtual" in source.resources:
        source.fact_virtual.apply_hints(write_disposition="replace")
        
    return source


@dlt.source(name="csv_to_postgres")
def csv_to_postgres_source():
    """DLT source: CSV → PostgreSQL."""
    yield fact_virtual_csv_resource()
