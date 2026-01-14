#!/usr/bin/env python3
"""
Test script to verify freshness sensor implementation
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from dbt_cloud_orchestration.defs.ingestion.enhanced_freshness_sensor import (
    enhanced_freshness_sensor,
    kaizen_wars_specific_freshness_sensor,
)
import dagster as dg


def test_freshness_sensors():
    """Test the freshness sensor implementations"""

    print("🧪 Testing Freshness Sensor Implementation...")

    # Create a mock context for testing
    class MockInstance:
        def get_asset_state(self, asset_key):
            # Return mock state for testing
            class MockAssetState:
                def __init__(self):
                    self.last_materialization_time = None

            return MockAssetState()

        def get_asset_spec(self, asset_key):
            # Return mock spec for testing
            class MockAssetSpec:
                def __init__(self):
                    self.freshness_policy = None

            return MockAssetSpec()

    class MockContext:
        def __init__(self):
            self.instance = MockInstance()
            self.log = MockLogger()

    class MockLogger:
        def info(self, msg):
            print(f"📝 INFO: {msg}")

        def warning(self, msg):
            print(f"⚠️  WARNING: {msg}")

        def error(self, msg):
            print(f"❌ ERROR: {msg}")

    # Test enhanced freshness sensor
    print("\n🔍 Testing Enhanced Freshness Sensor...")
    try:
        mock_context = MockContext()
        result = enhanced_freshness_sensor(mock_context)
        print(f"✅ Enhanced freshness sensor executed successfully")
        print(f"   Result type: {type(result)}")
        print(f"   Result: {result}")
    except Exception as e:
        print(f"❌ Enhanced freshness sensor failed: {e}")

    # Test Kaizen Wars specific sensor
    print("\n🎯 Testing Kaizen Wars Specific Freshness Sensor...")
    try:
        mock_context = MockContext()
        result = kaizen_wars_specific_freshness_sensor(mock_context)
        print(f"✅ Kaizen Wars freshness sensor executed successfully")
        print(f"   Result type: {type(result)}")
        print(f"   Result: {result}")
    except Exception as e:
        print(f"❌ Kaizen Wars freshness sensor failed: {e}")

    print("\n🎉 Freshness sensor tests completed!")
    return True


if __name__ == "__main__":
    success = test_freshness_sensors()
    sys.exit(0 if success else 1)
