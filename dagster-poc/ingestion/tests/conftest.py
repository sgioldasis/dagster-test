"""Pytest configuration and fixtures for ingestion tests."""

import os
from pathlib import Path

import pytest


@pytest.fixture
def test_data_dir() -> Path:
    """Provide the path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Set up environment variables for testing."""
    # Set required environment variables for tests
    monkeypatch.setenv("CSV_DATA_PATH", str(Path(__file__).parent / "data"))
    monkeypatch.setenv("LOCAL_POSTGRES_HOST", "localhost")
    monkeypatch.setenv("LOCAL_POSTGRES_PORT", "5432")
    monkeypatch.setenv("LOCAL_POSTGRES_USER", "test")
    monkeypatch.setenv("LOCAL_POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("LOCAL_POSTGRES_DATABASE", "test_db")
