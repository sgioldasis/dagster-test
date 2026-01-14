#!/usr/bin/env python3
"""
Manual sensor trigger script for testing freshness sensors
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from dbt_cloud_orchestration.defs.ingestion.definitions import defs
import dagster as dg


def trigger_sensor_manually(sensor_name):
    """Manually trigger a sensor for testing."""
    print(f"🔄 Manually triggering sensor: {sensor_name}")

    try:
        # Get Dagster instance
        instance = dg.DagsterInstance.get()

        # Find sensor definition
        sensor_def = None
        for sensor in defs.sensors:
            if sensor.name == sensor_name:
                sensor_def = sensor
                break

        if not sensor_def:
            print(f"❌ Sensor '{sensor_name}' not found")
            return False

        # Create evaluation context
        context = dg.build_sensor_context(
            instance=instance,
            repository_def=defs.get_repository_def(),
        )

        # Execute sensor
        result = sensor_def.evaluation_fn(context)
        print(f"✅ Sensor '{sensor_name}' executed successfully")
        print(f"   Result type: {type(result)}")
        return True

    except Exception as e:
        print(f"❌ Error triggering sensor '{sensor_name}': {e}")
        return False


def list_sensors():
    """List all available sensors"""
    print("📋 Available sensors:")
    for sensor in defs.sensors:
        print(
            f"  - {sensor.name} (interval: {getattr(sensor, 'minimum_interval_seconds', 'Not set')}s)"
        )


def check_sensor_history(sensor_name=None):
    """Check sensor execution history"""
    try:
        instance = dg.DagsterInstance.get()

        if sensor_name:
            # Get runs for specific sensor
            sensor_runs = instance.get_sensor_runs(sensor_name=sensor_name)
        else:
            # Get all sensor runs
            sensor_runs = instance.get_sensor_runs()

        print(f"📊 Sensor execution history ({len(sensor_runs)} runs):")
        for run in sensor_runs[:10]:  # Show last 10 runs
            print(
                f"  - {run.run_id}: {run.sensor_name} - {run.status} - {run.timestamp}"
            )

        return True

    except Exception as e:
        print(f"❌ Error checking sensor history: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manual sensor trigger script")
    parser.add_argument(
        "--sensor", help="Sensor name to trigger", default="simple_freshness_sensor"
    )
    parser.add_argument("--list", action="store_true", help="List available sensors")
    parser.add_argument(
        "--history", action="store_true", help="Check sensor execution history"
    )
    parser.add_argument("--test", action="store_true", help="Test sensor execution")

    args = parser.parse_args()

    if args.list:
        list_sensors()
    elif args.history:
        check_sensor_history(args.sensor)
    elif args.test:
        success = trigger_sensor_manually(args.sensor)
        if success:
            print("🎉 Sensor test completed successfully!")
        else:
            print("💥 Sensor test failed!")
            sys.exit(1)
    else:
        # Default: trigger the sensor
        success = trigger_sensor_manually(args.sensor)
        if success:
            print("🎉 Sensor trigger completed successfully!")
        else:
            print("💥 Sensor trigger failed!")
            sys.exit(1)
