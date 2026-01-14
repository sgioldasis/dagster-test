# tests/test_freshness_sensor.py

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import dagster as dg
from dagster._core.definitions.freshness_policy import FreshnessPolicyEvaluationData
from dbt_cloud_orchestration.defs.ingestion.freshness_sensor import kaizen_wars_freshness_sensor
from dbt_cloud_orchestration.defs.ingestion.freshness_utils import FreshnessMonitor, create_freshness_asset_key


class TestFreshnessSensor:
    """Test cases for the freshness sensor implementation."""
    
    def test_create_freshness_asset_key(self):
        """Test asset key creation utility function."""
        asset_key = create_freshness_asset_key("dlt_kaizen_wars_fact_virtual")
        expected_key = dg.AssetKey(["ingestion", "dlt_kaizen_wars_fact_virtual"])
        assert asset_key == expected_key
    
    @patch('dbt_cloud_orchestration.defs.ingestion.freshness_sensor.datetime')
    def test_freshness_sensor_fresh_asset(self, mock_datetime):
        """Test freshness sensor when asset is fresh."""
        # Setup mock data
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock asset state (recently materialized)
        mock_asset_state = Mock()
        mock_asset_state.last_materialization_time = datetime(2023, 1, 1, 11, 59, 0)
        
        # Mock asset spec with freshness policy
        mock_freshness_policy = dg.FreshnessPolicy.cron(
            deadline_cron="*/1 * * * *",
            lower_bound_delta=timedelta(minutes=1),
        )
        mock_asset_spec = Mock()
        mock_asset_spec.freshness_policy = mock_freshness_policy
        
        # Mock Dagster instance
        mock_instance = Mock()
        mock_instance.get_asset_state.return_value = mock_asset_state
        mock_instance.get_asset_spec.return_value = mock_asset_spec
        
        # Create mock context
        mock_context = Mock()
        mock_context.instance = mock_instance
        mock_context.log = Mock()
        
        # Run sensor
        result = kaizen_wars_freshness_sensor(mock_context)
        
        # Verify results
        assert result.asset_key == dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])
        assert result.current_time == mock_now
        assert result.last_materialization_time == mock_asset_state.last_materialization_time
        assert result.freshness_evaluation_data is not None
        assert "Fresh" in result.reason
    
    @patch('dbt_cloud_orchestration.defs.ingestion.freshness_sensor.datetime')
    def test_freshness_sensor_stale_asset(self, mock_datetime):
        """Test freshness sensor when asset is stale."""
        # Setup mock data
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock asset state (old materialization)
        mock_asset_state = Mock()
        mock_asset_state.last_materialization_time = datetime(2023, 1, 1, 11, 0, 0)
        
        # Mock asset spec with freshness policy
        mock_freshness_policy = dg.FreshnessPolicy.cron(
            deadline_cron="*/1 * * * *",
            lower_bound_delta=timedelta(minutes=1),
        )
        mock_asset_spec = Mock()
        mock_asset_spec.freshness_policy = mock_freshness_policy
        
        # Mock Dagster instance
        mock_instance = Mock()
        mock_instance.get_asset_state.return_value = mock_asset_state
        mock_instance.get_asset_spec.return_value = mock_asset_spec
        
        # Create mock context
        mock_context = Mock()
        mock_context.instance = mock_instance
        mock_context.log = Mock()
        
        # Run sensor
        result = kaizen_wars_freshness_sensor(mock_context)
        
        # Verify results
        assert result.asset_key == dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])
        assert result.current_time == mock_now
        assert result.last_materialization_time == mock_asset_state.last_materialization_time
        assert result.freshness_evaluation_data is not None
        assert "Stale" in result.reason
    
    def test_freshness_sensor_missing_asset(self):
        """Test freshness sensor when asset is missing."""
        # Mock Dagster instance returning None
        mock_instance = Mock()
        mock_instance.get_asset_state.return_value = None
        
        # Create mock context
        mock_context = Mock()
        mock_context.instance = mock_instance
        mock_context.log = Mock()
        
        # Run sensor
        result = kaizen_wars_freshness_sensor(mock_context)
        
        # Verify results
        assert result.asset_key == dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])
        assert result.skip_reason is not None
        assert "not found" in result.reason


class TestFreshnessMonitor:
    """Test cases for the freshness monitor utility."""
    
    def test_freshness_monitor_creation(self):
        """Test FreshnessMonitor creation."""
        mock_instance = Mock()
        monitor = FreshnessMonitor(mock_instance)
        assert monitor.instance == mock_instance
    
    def test_get_freshness_status_fresh(self):
        """Test freshness status calculation for fresh asset."""
        # Mock asset state (recently materialized)
        mock_asset_state = Mock()
        mock_asset_state.last_materialization_time = datetime.now() - timedelta(minutes=30)
        
        # Mock asset spec with freshness policy
        mock_freshness_policy = dg.FreshnessPolicy.cron(
            deadline_cron="*/1 * * * *",
            lower_bound_delta=timedelta(hours=1),
        )
        mock_asset_spec = Mock()
        mock_spec = Mock()
        mock_spec.freshness_policy = mock_freshness_policy
        mock_asset_spec.return_value = mock_spec
        
        # Mock Dagster instance
        mock_instance = Mock()
        mock_instance.get_asset_state.return_value = mock_asset_state
        mock_instance.get_asset_spec.return_value = mock_asset_spec
        
        # Test freshness monitor
        monitor = FreshnessMonitor(mock_instance)
        asset_key = dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])
        status = monitor.get_freshness_status(asset_key)
        
        # Verify status
        assert status["status"] == "fresh"
        assert status["color"] == "green"
        assert "✅" in status["description"]
        assert status["last_materialization"] is not None
    
    def test_get_freshness_status_stale(self):
        """Test freshness status calculation for stale asset."""
        # Mock asset state (old materialization)
        mock_asset_state = Mock()
        mock_asset_state.last_materialization_time = datetime.now() - timedelta(hours=2)
        
        # Mock asset spec with freshness policy
        mock_freshness_policy = dg.FreshnessPolicy.cron(
            deadline_cron="*/1 * * * *",
            lower_bound_delta=timedelta(hours=1),
        )
        mock_asset_spec = Mock()
        mock_spec = Mock()
        mock_spec.freshness_policy = mock_freshness_policy
        mock_asset_spec.return_value = mock_spec
        
        # Mock Dagster instance
        mock_instance = Mock()
        mock_instance.get_asset_state.return_value = mock_asset_state
        mock_instance.get_asset_spec.return_value = mock_asset_spec
        
        # Test freshness monitor
        monitor = FreshnessMonitor(mock_instance)
        asset_key = dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])
        status = monitor.get_freshness_status(asset_key)
        
        # Verify status
        assert status["status"] == "stale"
        assert status["color"] == "red"
        assert "⚠️" in status["description"]
        assert status["last_materialization"] is not None
    
    def test_get_freshness_status_missing(self):
        """Test freshness status calculation for missing asset."""
        # Mock Dagster instance returning None
        mock_instance = Mock()
        mock_instance.get_asset_state.return_value = None
        
        # Test freshness monitor
        monitor = FreshnessMonitor(mock_instance)
        asset_key = dg.AssetKey(["dlt_kaizen_wars_fact_virtual"])
        status = monitor.get_freshness_status(asset_key)
        
        # Verify status
        assert status["status"] == "missing"
        assert "❌" in status["description"]
        assert status["last_materialization"] is None


class TestFreshnessIntegration:
    """Integration tests for freshness sensor components."""
    
    def test_sensor_integration(self):
        """Test that all sensor components work together."""
        # This test verifies that the sensor can be imported and instantiated
        from dbt_cloud_orchestration.defs.ingestion.freshness_sensor import kaizen_wars_freshness_sensor_def
        from dbt_cloud_orchestration.defs.ingestion.enhanced_freshness_sensor import enhanced_freshness_sensor_def
        
        # Verify sensor definitions are properly created
        assert kaizen_wars_freshness_sensor_def is not None
        assert enhanced_freshness_sensor_def is not None
        
        # Verify sensor properties
        assert kaizen_wars_freshness_sensor_def.name == "kaizen_wars_freshness_sensor"
        assert enhanced_freshness_sensor_def.name == "enhanced_freshness_sensor"
        
        # Verify evaluation functions are callable
        assert callable(kaizen_wars_freshness_sensor_def.evaluation_fn)
        assert callable(enhanced_freshness_sensor_def.evaluation_fn)


if __name__ == "__main__":
    pytest.main([__file__])
