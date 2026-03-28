"""Configuration management for the ingestion project.

This module provides Pydantic-based configuration validation that loads
environment variables at startup and validates required settings.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionSettings(BaseSettings):
    """Ingestion project settings loaded from environment variables.
    
    All settings are loaded from environment variables at startup.
    For local development, create a .env file in the project root.
    
    Example .env:
        CSV_DATA_PATH=/path/to/data
        LOCAL_POSTGRES_HOST=localhost
        LOCAL_POSTGRES_PORT=5432
        LOCAL_POSTGRES_USER=myuser
        LOCAL_POSTGRES_PASSWORD=mypass
        LOCAL_POSTGRES_DATABASE=mydb
        DATABRICKS_HOST=adb-xxx.azuredatabricks.net
        DATABRICKS_TOKEN=dapi-xxx
        DATABRICKS_WAREHOUSE_ID=xxx
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Allow extra env vars not defined here
    )
    
    # CSV Data Configuration
    csv_data_path: Path = Path("./data")
    
    # PostgreSQL Configuration (local)
    local_postgres_host: str = "localhost"
    local_postgres_port: int = 5432
    local_postgres_user: str = "postgres"
    local_postgres_password: str = ""
    local_postgres_database: str = "postgres"
    
    # Databricks Configuration (optional)
    databricks_host: str | None = None
    databricks_token: str | None = None
    databricks_warehouse_id: str | None = None
    databricks_http_path: str | None = None
    databricks_catalog: str = "test"
    databricks_schema: str = "main"
    databricks_notebook_job_id: str | None = None
    
    @property
    def postgres_connection_string(self) -> str:
        """Build PostgreSQL connection string from settings."""
        return (
            f"postgresql://{self.local_postgres_user}:{self.local_postgres_password}"
            f"@{self.local_postgres_host}:{self.local_postgres_port}"
            f"/{self.local_postgres_database}"
        )
    
    @property
    def databricks_configured(self) -> bool:
        """Check if Databricks credentials are fully configured."""
        return all([
            self.databricks_host,
            self.databricks_token,
            self.databricks_warehouse_id,
        ])


@lru_cache
def get_settings() -> IngestionSettings:
    """Get cached settings instance.
    
    Settings are loaded once and cached for the application lifetime.
    This avoids re-reading environment variables on every access.
    
    Returns:
        Validated IngestionSettings instance.
    """
    return IngestionSettings()
