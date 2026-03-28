"""Tests for configuration module."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from ingestion.config import IngestionSettings, get_settings


class TestIngestionSettings:
    """Test cases for IngestionSettings configuration."""

    def test_explicit_values(self):
        """Test that explicitly set values are used correctly."""
        settings = IngestionSettings(
            local_postgres_host="customhost",
            local_postgres_port=5433,
            local_postgres_user="customuser",
            local_postgres_password="custompass",
            local_postgres_database="customdb",
            databricks_catalog="custom_catalog",
            databricks_schema="custom_schema",
        )

        assert settings.local_postgres_host == "customhost"
        assert settings.local_postgres_port == 5433
        assert settings.local_postgres_user == "customuser"
        assert settings.local_postgres_password == "custompass"
        assert settings.local_postgres_database == "customdb"
        assert settings.databricks_catalog == "custom_catalog"
        assert settings.databricks_schema == "custom_schema"

    def test_postgres_connection_string(self):
        """Test that connection string is built correctly."""
        settings = IngestionSettings(
            local_postgres_host="myhost",
            local_postgres_port=5432,
            local_postgres_user="myuser",
            local_postgres_password="mypass",
            local_postgres_database="mydb",
        )

        expected = "postgresql://myuser:mypass@myhost:5432/mydb"
        assert settings.postgres_connection_string == expected

    def test_databricks_configured_true(self):
        """Test that databricks_configured returns True when all vars set."""
        settings = IngestionSettings(
            databricks_host="adb-123.azuredatabricks.net",
            databricks_token="dapi-xxx",
            databricks_warehouse_id="12345",
        )

        assert settings.databricks_configured is True

    def test_databricks_configured_false(self):
        """Test that databricks_configured returns False when vars missing."""
        settings = IngestionSettings(
            databricks_host="adb-123.azuredatabricks.net",
            databricks_token=None,
            databricks_warehouse_id="12345",
        )

        assert settings.databricks_configured is False

    def test_csv_data_path_as_path(self):
        """Test that csv_data_path is converted to Path."""
        settings = IngestionSettings(csv_data_path="/some/path")

        assert isinstance(settings.csv_data_path, Path)
        assert str(settings.csv_data_path) == "/some/path"


import os
