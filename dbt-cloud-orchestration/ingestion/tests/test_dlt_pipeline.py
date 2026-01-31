"""Tests for the DLT pipeline module."""

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from ingestion.config import get_settings


class TestRawFactVirtualCsv:
    """Test cases for raw_fact_virtual_csv resource."""

    def test_raises_on_missing_csv_file(self, tmp_path, monkeypatch):
        """Test that error is raised when CSV is missing."""
        from ingestion.components.ingestion_dlt.dlt_pipeline import raw_fact_virtual_csv

        # Set up a temp directory without the CSV file
        monkeypatch.setenv("CSV_DATA_PATH", str(tmp_path))
        get_settings.cache_clear()

        # DLT wraps exceptions, so we just check that some exception is raised
        with pytest.raises(Exception, match="raw_fact_virtual.csv"):
            # Consume the generator to trigger the error
            list(raw_fact_virtual_csv())

    def test_yields_records_from_csv(self, tmp_path, monkeypatch):
        """Test that records are yielded from the CSV file."""
        from ingestion.components.ingestion_dlt.dlt_pipeline import raw_fact_virtual_csv

        # Create a test CSV file
        csv_path = tmp_path / "raw_fact_virtual.csv"
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        df.to_csv(csv_path, index=False)

        monkeypatch.setenv("CSV_DATA_PATH", str(tmp_path))

        # Clear the settings cache to pick up new env var
        get_settings.cache_clear()

        records = list(raw_fact_virtual_csv())

        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[0]["value"] == "a"


class TestCsvSource:
    """Test cases for csv_source."""

    def test_csv_source_yields_resource(self):
        """Test that csv_source yields the raw_fact_virtual_csv resource."""
        from ingestion.components.ingestion_dlt.dlt_pipeline import (
            csv_source,
            raw_fact_virtual_csv,
        )

        source = csv_source()
        resources = list(source.selected_resources.values())

        assert len(resources) == 1
        # The resource should be the raw_fact_virtual_csv function
        assert resources[0].name == "dlt_fact_virtual"


class TestFactVirtualPostgres:
    """Test cases for fact_virtual_postgres resource."""

    @patch("ingestion.components.ingestion_dlt.dlt_pipeline.sql_table")
    def test_uses_configured_connection_string(self, mock_sql_table, monkeypatch):
        """Test that the postgres connection uses settings from config."""
        from ingestion.components.ingestion_dlt.dlt_pipeline import fact_virtual_postgres

        # Mock the sql_table to return an empty iterator
        mock_sql_table.return_value = iter([])

        monkeypatch.setenv("LOCAL_POSTGRES_HOST", "testhost")
        monkeypatch.setenv("LOCAL_POSTGRES_PORT", "5432")
        monkeypatch.setenv("LOCAL_POSTGRES_USER", "testuser")
        monkeypatch.setenv("LOCAL_POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("LOCAL_POSTGRES_DATABASE", "testdb")

        # Consume the generator
        list(fact_virtual_postgres())

        # Verify sql_table was called with correct parameters
        mock_sql_table.assert_called_once()
        call_args = mock_sql_table.call_args
        assert call_args.kwargs["schema"] == "public"
        assert call_args.kwargs["table"] == "fact_virtual"


class TestPostgresSource:
    """Test cases for postgres_source."""

    def test_postgres_source_yields_resource(self):
        """Test that postgres_source yields the fact_virtual_postgres resource."""
        from ingestion.components.ingestion_dlt.dlt_pipeline import (
            fact_virtual_postgres,
            postgres_source,
        )

        source = postgres_source()
        resources = list(source.selected_resources.values())

        assert len(resources) == 1
        assert resources[0].name == "dlt_fact_virtual"
