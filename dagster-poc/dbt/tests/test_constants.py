"""Tests for constants module."""

import sys
from pathlib import Path

# Add src to path for direct imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Direct import to avoid dagster-dbt dependency
from dbt_orchestration.defs.constants import DbtCloudRunStatus


class TestDbtCloudRunStatus:
    """Test cases for DbtCloudRunStatus enum."""

    def test_status_values(self):
        """Test that status codes match expected values."""
        assert DbtCloudRunStatus.QUEUED == 1
        assert DbtCloudRunStatus.STARTING == 2
        assert DbtCloudRunStatus.RUNNING == 3
        assert DbtCloudRunStatus.SUCCESS == 10
        assert DbtCloudRunStatus.ERROR == 20
        assert DbtCloudRunStatus.CANCELLED == 30

    def test_is_running(self):
        """Test is_running method."""
        assert DbtCloudRunStatus.is_running(1) is True
        assert DbtCloudRunStatus.is_running(2) is True
        assert DbtCloudRunStatus.is_running(3) is True
        assert DbtCloudRunStatus.is_running(10) is False
        assert DbtCloudRunStatus.is_running(20) is False
        assert DbtCloudRunStatus.is_running(30) is False

    def test_is_terminal(self):
        """Test is_terminal method."""
        assert DbtCloudRunStatus.is_terminal(1) is False
        assert DbtCloudRunStatus.is_terminal(2) is False
        assert DbtCloudRunStatus.is_terminal(3) is False
        assert DbtCloudRunStatus.is_terminal(10) is True
        assert DbtCloudRunStatus.is_terminal(20) is True
        assert DbtCloudRunStatus.is_terminal(30) is True
