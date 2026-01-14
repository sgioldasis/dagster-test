#!/usr/bin/env python3
"""
Run script for freshness sensor demonstration.

This script demonstrates how to use the freshness sensor implementation
and provides a simple way to test the freshness monitoring functionality.
"""

import os
import sys
import argparse
from datetime import datetime, timedelta

# Add the source directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import dagster as dg
from dbt_cloud_orchestration.defs.ingestion.freshness_utils import FreshnessMonitor, create_freshness_asset_key


def create_test_instance():
    """Create a test Dagster instance for demonstration purposes."""
    # Use the default instance or create a test one
    try:
        instance = dg.DagsterInstance.get()
    except Exception:
        # Create a temporary instance for testing
        instance = dg.DagsterInstance.get()
    return instance


def demonstrate_freshness_monitoring():
    """Demonstrate freshness monitoring functionality."""
    print("🔍 Freshness Sensor Demonstration")
    print("=" * 50)
    
    # Create test instance
    instance = create_test_instance()
    
    # Create freshness monitor
    monitor = FreshnessMonitor(instance)
    
    # Target assets for monitoring
    target_assets = [
        create_freshness_asset_key("dlt_csv_data_source_customers"),
        create_freshness_asset_key("dlt_csv_data_source_orders"),
        create_freshness_asset_key("dlt_csv_data_source_payments"),
        create_freshness_asset_key("dlt_kaizen_wars_fact_virtual"),
    ]
    
    print(f"\n📊 Monitoring {len(target_assets)} assets:")
    print("-" * 30)
    
    # Get freshness summary
    summary = monitor.get_freshness_summary(target_assets)
    
    # Display overall summary
    print(f"\n📈 Overall Summary:")
    print(f"   Total Assets: {summary['total_assets']}")
    print(f"   Fresh: {summary['fresh_count']}")
    print(f"   Stale: {summary['stale_count']}")
    print(f"   Missing: {summary['missing_count']}")
    print(f"   Unknown: {summary['unknown_count']}")
    print(f"   Errors: {summary['error_count']}")
    print(f"   Overall Status: {summary['overall_status'].upper()}")
    
    # Display individual asset status
    print(f"\n🔍 Individual Asset Status:")
    print("-" * 30)
    
    for asset_key_str, asset_info in summary["assets"].items():
        color_emoji = {
            "green": "🟢",
            "red": "🔴", 
            "yellow": "🟡",
            "gray": "⚫",
            "unknown": "❓",
        }.get(asset_info["color"], "❓")
        
        print(f"   {color_emoji} {asset_key_str}")
        print(f"      Status: {asset_info['status'].upper()}")
        print(f"      Description: {asset_info['description']}")
        
        if asset_info["last_materialization"]:
            print(f"      Last Materialization: {asset_info['last_materialization']}")
        
        print()
    
    return summary


def test_freshness_sensor():
    """Test the freshness sensor functionality."""
    print("\n⚙️ Freshness Sensor Test")
    print("=" * 50)
    
    try:
        # Import the sensor
        from dbt_cloud_orchestration.defs.ingestion.freshness_sensor import kaizen_wars_freshness_sensor
        
        # Create a mock context for testing
        class MockContext:
            def __init__(self):
                self.instance = create_test_instance()
                self.log = MockLog()
        
        class MockLog:
            def info(self, msg): print(f"INFO: {msg}")
            def warning(self, msg): print(f"WARNING: {msg}")
            def error(self, msg): print(f"ERROR: {msg}")
        
        # Test the sensor
        context = MockContext()
        result = kaizen_wars_freshness_sensor(context)
        
        print(f"\n✅ Sensor Test Results:")
        print(f"   Asset Key: {result.asset_key}")
        print(f"   Current Time: {result.current_time}")
        print(f"   Last Materialization: {result.last_materialization_time}")
        print(f"   Reason: {result.reason}")
        
        if result.skip_reason:
            print(f"   Skip Reason: {result.skip_reason}")
        
        return True
        
    except Exception as e:
        print(f"❌ Sensor Test Failed: {str(e)}")
        return False


def main():
    """Main function to run the freshness sensor demonstration."""
    parser = argparse.ArgumentParser(description="Freshness Sensor Demonstration")
    parser.add_argument("--monitor", action="store_true", help="Run freshness monitoring demo")
    parser.add_argument("--test", action="store_true", help="Test freshness sensor functionality")
    parser.add_argument("--all", action="store_true", help="Run all demonstrations")
    
    args = parser.parse_args()
    
    if args.all or not (args.monitor or args.test):
        args.monitor = True
        args.test = True
    
    if args.monitor:
        demonstrate_freshness_monitoring()
    
    if args.test:
        test_freshness_sensor()
    
    print("\n🎉 Freshness Sensor Demonstration Complete!")
    print("\n💡 Next Steps:")
    print("   1. Navigate to the Dagster UI to view visual freshness indicators")
    print("   2. Check the 'Sensors' tab to monitor sensor execution")
    print("   3. Review asset lineage for freshness status")
    print("   4. Configure alerts for stale assets (optional)")


if __name__ == "__main__":
    main()
