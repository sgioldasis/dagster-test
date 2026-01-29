"""Utility functions for PostgreSQL connection management.

This module provides helper functions for building and parsing PostgreSQL
connection strings. It supports switching between different database targets
(local PostgreSQL vs Supabase) via environment variables.

Environment-Based Configuration:
    The INGESTION_TARGET environment variable controls which configuration
    is used:
    - "local" (default): Connect to local PostgreSQL instance
    - "supabase": Connect to Supabase PostgreSQL

Required Environment Variables:
    For local development:
        LOCAL_POSTGRES_HOST (default: localhost)
        LOCAL_POSTGRES_PORT (default: 5432)
        LOCAL_POSTGRES_USER (default: current user)
        LOCAL_POSTGRES_PASSWORD (default: empty)
        LOCAL_POSTGRES_DATABASE (default: postgres)

    For Supabase:
        SUPABASE_HOST
        SUPABASE_PORT (default: 5432)
        SUPABASE_USER
        SUPABASE_PASSWORD
        SUPABASE_DATABASE (default: postgres)

Example Usage:
    ```python
    from ingestion.defs.utils import get_postgres_connection_string

    # Reads from environment variables
    conn_string = get_postgres_connection_string()
    # Result: postgresql://user:pass@localhost:5432/mydb
    ```
"""

import getpass
from urllib.parse import urlparse

from dagster import EnvVar


def get_postgres_connection_string() -> str:
    """Build a PostgreSQL connection string from environment variables.

    This function supports switching between different database targets
    based on the INGESTION_TARGET environment variable. It's useful for
    switching between local development and cloud databases without code changes.

    Environment Variables:
        INGESTION_TARGET: "local" or "supabase" (default: "local")

    Returns:
        PostgreSQL connection string in the format:
        postgresql://username[:password]@host:port/database

    Example:
        ```python
        # With INGESTION_TARGET=local
        conn = get_postgres_connection_string()
        # Returns: postgresql://savas@localhost:5432/postgres

        # With INGESTION_TARGET=supabase
        conn = get_postgres_connection_string()
        # Returns: postgresql://postgres.xxx@aws.pooler.supabase.com:5432/postgres
        ```
    """
    target = EnvVar("INGESTION_TARGET").get_value() or "local"

    if target.lower() == "supabase":
        # Supabase PostgreSQL configuration
        host = EnvVar("SUPABASE_HOST").get_value() or "aws-1-eu-west-1.pooler.supabase.com"
        port = EnvVar("SUPABASE_PORT").get_value() or "5432"
        user = EnvVar("SUPABASE_USER").get_value() or "postgres.optokmygftwwajposhdy"
        password = EnvVar("SUPABASE_PASSWORD").get_value() or ""
        database = EnvVar("SUPABASE_DATABASE").get_value() or "postgres"
    else:
        # Local PostgreSQL configuration (default)
        host = EnvVar("LOCAL_POSTGRES_HOST").get_value() or "localhost"
        port = EnvVar("LOCAL_POSTGRES_PORT").get_value() or "5432"
        # Use current system username as default
        user = EnvVar("LOCAL_POSTGRES_USER").get_value() or getpass.getuser()
        password = EnvVar("LOCAL_POSTGRES_PASSWORD").get_value() or ""
        database = EnvVar("LOCAL_POSTGRES_DATABASE").get_value() or "postgres"

    # Build the connection string
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        return f"postgresql://{user}@{host}:{port}/{database}"


def parse_postgres_connection_string(conn_string: str) -> dict:
    """Parse a PostgreSQL connection string into its components.

    This is useful when you need to extract individual connection parameters
    from a connection string, for example to configure a client that doesn't
    accept connection string format.

    Args:
        conn_string: PostgreSQL connection string in the format:
            postgresql://username[:password]@host:port/database

    Returns:
        Dictionary with keys: host, port, username, password, database

    Example:
        ```python
        conn = "postgresql://user:pass@localhost:5432/mydb"
        parsed = parse_postgres_connection_string(conn)
        # Returns:
        # {
        #     "host": "localhost",
        #     "port": 5432,
        #     "username": "user",
        #     "password": "pass",
        #     "database": "mydb"
        # }
        ```
    """
    parsed = urlparse(conn_string)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "username": parsed.username,
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/"),
    }
