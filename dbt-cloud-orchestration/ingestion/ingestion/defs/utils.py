import getpass
from urllib.parse import urlparse
from dagster import EnvVar

def get_postgres_connection_string():
    """Build PostgreSQL connection string from env vars.
    Supports switching between local Postgres and Supabase via INGESTION_TARGET env var.
    """
    target = EnvVar("INGESTION_TARGET").get_value() or "local"
    
    if target.lower() == "supabase":
        host = EnvVar("SUPABASE_HOST").get_value() or "aws-1-eu-west-1.pooler.supabase.com"
        port = EnvVar("SUPABASE_PORT").get_value() or "5432"
        user = EnvVar("SUPABASE_USER").get_value() or "postgres.optokmygftwwajposhdy"
        password = EnvVar("SUPABASE_PASSWORD").get_value() or ""
        database = EnvVar("SUPABASE_DATABASE").get_value() or "postgres"
    else:
        host = EnvVar("LOCAL_POSTGRES_HOST").get_value() or "localhost"
        port = EnvVar("LOCAL_POSTGRES_PORT").get_value() or "5432"
        user = EnvVar("LOCAL_POSTGRES_USER").get_value() or getpass.getuser()
        password = EnvVar("LOCAL_POSTGRES_PASSWORD").get_value() or ""
        database = EnvVar("LOCAL_POSTGRES_DATABASE").get_value() or "postgres"

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        return f"postgresql://{user}@{host}:{port}/{database}"


def parse_postgres_connection_string(conn_string: str) -> dict:
    """Parse PostgreSQL connection string into a dictionary of credentials."""
    parsed = urlparse(conn_string)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "username": parsed.username,
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/"),
    }
