# ingestion/src/ingestion/defs/dlt_pipeline.py
"""DLT resources and source definitions for Kaizen Wars data ingestion."""

import dlt
import pandas as pd
import getpass
from dagster import EnvVar


def get_postgres_connection_string():
    """Build PostgreSQL connection string from env vars. Uses local Postgres by default."""
    host = EnvVar("LOCAL_POSTGRES_HOST").get_value() or "localhost"
    port = EnvVar("LOCAL_POSTGRES_PORT").get_value() or "5432"
    user = EnvVar("LOCAL_POSTGRES_USER").get_value() or getpass.getuser()
    password = EnvVar("LOCAL_POSTGRES_PASSWORD").get_value() or ""
    database = EnvVar("LOCAL_POSTGRES_DATABASE").get_value() or "postgres"

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        return f"postgresql://{user}@{host}:{port}/{database}"


def get_supabase_connection_string():
    """Build Supabase PostgreSQL connection string from env vars."""
    host = EnvVar("SUPABASE_HOST").get_value() or "aws-1-eu-west-1.pooler.supabase.com"
    port = EnvVar("SUPABASE_PORT").get_value() or "5432"
    user = EnvVar("SUPABASE_USER").get_value() or "postgres.optokmygftwwajposhdy"
    password = EnvVar("SUPABASE_PASSWORD").get_value() or "SupaBigbro14!!"
    database = EnvVar("SUPABASE_DATABASE").get_value() or "postgres"
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


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


@dlt.resource(
    name="fact_virtual",
    write_disposition="replace",
)
def postgres_resource(table_name: str = "fact_virtual"):
    """Load all data from PostgreSQL (full reload every run)."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn_string = get_postgres_connection_string()

    try:
        conn = psycopg2.connect(conn_string)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            for row in cursor:
                yield dict(row)
        conn.close()
    except Exception as e:
        print(f"[DEBUG] Error reading from PostgreSQL: {e}")
        yield from []


def supabase_postgres_resource(table_name: str = "fact_virtual"):
    """Load data from Supabase PostgreSQL (legacy - kept for reference)."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn_string = get_supabase_connection_string()

    try:
        conn = psycopg2.connect(conn_string)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            for row in cursor:
                yield dict(row)
        conn.close()
    except Exception as e:
        print(f"[DEBUG] Error reading from Supabase: {e}")
        yield from []


@dlt.source(name="postgres")
def postgres_source():
    """DLT source: PostgreSQL (local or Supabase) → Databricks."""
    yield postgres_resource


@dlt.source(name="csv_to_postgres")
def csv_to_postgres_source():
    """DLT source: CSV → PostgreSQL (local or Supabase)."""
    yield fact_virtual_csv_resource


def supabase_source():
    """DLT source: Supabase PostgreSQL → Databricks (legacy)."""
    yield supabase_postgres_resource
