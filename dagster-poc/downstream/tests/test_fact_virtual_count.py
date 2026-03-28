"""Tests for the fact_virtual_count asset."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from downstream.defs.fact_virtual_count import (
    _find_successful_run_id,
    _get_fact_table_count_from_api,
    _validate_config,
    _write_count_output,
    fact_virtual_count_asset,
)


class TestValidateConfig:
    """Tests for configuration validation."""

    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCOUNT_ID", 12345)
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_TOKEN", "test-token")
    def test_valid_config(self):
        """Test that valid configuration passes validation."""
        context = build_asset_context()
        _validate_config(context)  # Should not raise

    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCOUNT_ID", 0)
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_TOKEN", "test-token")
    def test_missing_account_id(self):
        """Test that missing account ID raises ValueError."""
        context = build_asset_context()
        with pytest.raises(ValueError, match="DBT_CLOUD_ACCOUNT_ID"):
            _validate_config(context)

    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCOUNT_ID", 12345)
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCESS_URL", "")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_TOKEN", "test-token")
    def test_missing_access_url(self):
        """Test that missing access URL raises ValueError."""
        context = build_asset_context()
        with pytest.raises(ValueError, match="DBT_CLOUD_ACCESS_URL"):
            _validate_config(context)

    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCOUNT_ID", 12345)
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_TOKEN", "")
    def test_missing_token(self):
        """Test that missing token raises ValueError."""
        context = build_asset_context()
        with pytest.raises(ValueError, match="DBT_CLOUD_TOKEN"):
            _validate_config(context)


class TestWriteCountOutput:
    """Tests for writing count output to file."""

    def test_write_output_creates_file(self):
        """Test that output file is created with correct content."""
        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "downstream.defs.fact_virtual_count.OUTPUT_DIR", Path(tmpdir)
        ):
            output_file = _write_count_output(
                fact_table_count=42,
                run_id=123,
                fact_table_name="test_table",
            )

            assert output_file.exists()
            with open(output_file) as f:
                data = json.load(f)

            assert data["fact_table"] == "test_table"
            assert data["count"] == 42
            assert data["run_id"] == 123
            assert "timestamp" in data


class TestGetFactTableCount:
    """Tests for getting fact table count from API."""

    def test_returns_mock_value(self):
        """Test that the mock implementation returns the expected value."""
        context = build_asset_context()
        result = _get_fact_table_count_from_api(
            account_id=123,
            token="test",
            access_url="https://cloud.getdbt.com",
            context=context,
        )
        assert result == 12345


class TestFindSuccessfulRunId:
    """Tests for finding successful run ID."""

    @patch("downstream.defs.fact_virtual_count.requests.get")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCOUNT_ID", 12345)
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_TOKEN", "test-token")
    def test_finds_successful_run(self, mock_get):
        """Test that successful run is found."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "status": 20},  # Failed
                {"id": 2, "status": 10},  # Success
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        context = build_asset_context()
        run_id = _find_successful_run_id(context)

        assert run_id == 2

    @patch("downstream.defs.fact_virtual_count.requests.get")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCOUNT_ID", 12345)
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_TOKEN", "test-token")
    def test_no_successful_run_raises(self, mock_get):
        """Test that RuntimeError is raised when no successful runs found."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "status": 20},  # Failed
                {"id": 2, "status": 30},  # Cancelled
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        context = build_asset_context()
        with pytest.raises(RuntimeError, match="No successful dbt Cloud runs"):
            _find_successful_run_id(context)


class TestFactVirtualCountAsset:
    """Integration tests for the fact_virtual_count_asset."""

    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCOUNT_ID", 12345)
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com")
    @patch("downstream.defs.fact_virtual_count.DBT_CLOUD_TOKEN", "test-token")
    @patch("downstream.defs.fact_virtual_count._find_successful_run_id")
    @patch("downstream.defs.fact_virtual_count._get_fact_table_count_from_api")
    def test_asset_execution(self, mock_get_count, mock_find_run):
        """Test the full asset execution."""
        mock_find_run.return_value = 123
        mock_get_count.return_value = 42

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "downstream.defs.fact_virtual_count.OUTPUT_DIR", Path(tmpdir)
        ):
            context = build_asset_context()
            result = fact_virtual_count_asset(context)

            assert isinstance(result, Path)
            assert result.exists()

            with open(result) as f:
                data = json.load(f)

            assert data["count"] == 42
            assert data["run_id"] == 123
