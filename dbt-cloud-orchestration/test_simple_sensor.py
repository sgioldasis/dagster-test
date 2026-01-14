#!/usr/bin/env python3
"""
Simple test script for freshness sensors
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from dbt_cloud_orchestration.defs.ingestion.simple_freshness_sensor import (
    simple_freshness_sensor,
)


class MockContext:
    """Mock sensor evaluation context"""

    def __init__(self):
        self.log = MockLogger()

    def log_info(self, msg):
        print(f"📝 INFO: {msg}")


class MockLogger:
    """Mock logger"""

    def info(self, msg):
        print(f"📝 INFO: {msg}")

    def warning(self, msg):
        print(f"⚠️  WARNING: {msg}")

    def error(self, msg):
        print(f"❌ ERROR: {msg}")


def test_sensor():
    """Test the freshness sensor"""
    print("🧪 Testing Freshness Sensor...")

    try:
        # Create mock context
        mock_context = MockContext()

        # Execute sensor
        result = simple_freshness_sensor(mock_context)

        print(f"✅ Sensor executed successfully")
        print(f"   Result type: {type(result)}")
        print(f"   Result: {result}")

        return True

    except Exception as e:
        print(f"❌ Sensor test failed: {e}")
        return False


if __name__ == "__main__":
    success = test_sensor()
    if success:
        print("\n🎉 Freshness sensor test completed successfully!")
    else:
        print("\n💥 Freshness sensor test failed!")
        sys.exit(1)
