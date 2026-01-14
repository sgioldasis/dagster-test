# Freshness Sensor Implementation for dlt_kaizen_wars_fact_virtual

## Overview

This implementation provides comprehensive freshness monitoring for the `dlt_kaizen_wars_fact_virtual` asset with the following features:

1. **Evaluates freshness every minute** - Automated freshness checking
2. **Visual indication in lineage view** - Color-coded status indicators
3. **Enhanced logging and monitoring** - Detailed status reporting
4. **Multiple sensor implementations** - Different approaches for flexibility

## Implementation Files

### 1. Core Files

- `src/dbt_cloud_orchestration/defs/ingestion/freshness_sensor.py`
  - Basic freshness sensor implementation
  - Uses Dagster's `FreshnessSensorDefinition`
  - Provides standard freshness evaluation

- `src/dbt_cloud_orchestration/defs/ingestion/freshness_utils.py`
  - Utility class for freshness monitoring
  - Helper functions for status calculation
  - Comprehensive status reporting

- `src/dbt_cloud_orchestration/defs/ingestion/enhanced_freshness_sensor.py`
  - Enhanced sensor with multiple asset support
  - Advanced visual feedback
  - Comprehensive monitoring capabilities

### 2. Definitions

- `src/dbt_cloud_orchestration/defs/ingestion/definitions.py`
  - Updated to include all freshness sensors
  - Maintains existing automation sensors
  - Integrates with existing schedules

## Sensor Types

### 1. FreshnessSensorDefinition (Recommended)

```python
kaizen_wars_freshness_sensor_def = dg.FreshnessSensorDefinition(
    name="kaizen_wars_freshness_sensor",
    monitorable_asset_selection=AssetSelection.keys("dlt_kaizen_wars_fact_virtual"),
    evaluation_fn=kaizen_wars_specific_freshness_sensor,
    minimum_interval_seconds=60,  # Every minute
)
```

**Features:**
- Built-in Dagster freshness evaluation
- Automatic visual indication in UI
- Standardized freshness status
- Minimal configuration required

### 2. Enhanced Sensor with Custom Logic

```python
enhanced_freshness_sensor_def = dg.SensorDefinition(
    name="enhanced_freshness_sensor",
    evaluation_fn=enhanced_freshness_sensor,
    minimum_interval_seconds=60,
)
```

**Features:**
- Custom evaluation logic
- Multiple asset support
- Enhanced metadata and logging
- Flexible alerting capabilities

## Freshness Status Indicators

### Visual Status in Lineage View

- **✅ Fresh** (Green) - Asset is within freshness threshold
- **⚠️ Stale** (Red) - Asset exceeds freshness threshold
- **❓ Unknown** (Yellow) - Asset or policy not configured
- **❌ Missing** (Gray) - No materialization found
- **❌ Error** (Red) - Error during evaluation

### Status Description Format

```
✅ Fresh - Updated 45s ago (threshold: 60s)
⚠️ Stale - Updated 125s ago (threshold: 60s)
❓ Unknown - No freshness policy configured
❌ Missing - No materialization found
❌ Error - Error checking freshness: ...
```

## Configuration

### Asset Freshness Policy

The `dlt_kaizen_wars_fact_virtual` asset already has a freshness policy configured:

```python
freshness_policy=dg.FreshnessPolicy.cron(
    deadline_cron="*/1 * * * *",  # Check freshness every minute
    lower_bound_delta=timedelta(minutes=1),
)
```

### Sensor Schedule

- **Evaluation Frequency**: Every 60 seconds
- **Target Asset**: `dlt_kaizen_wars_fact_virtual`
- **Integration**: Added to existing definitions

## Usage

### 1. Viewing Freshness Status

In the Dagster UI:
1. Navigate to the **Asset Lineage** view
2. Find the `dlt_kaizen_wars_fact_virtual` asset
3. Visual status indicators will show freshness
4. Click on the asset for detailed status information

### 2. Sensor Logs

Check sensor execution logs:
- **Sensor Name**: `kaizen_wars_freshness_sensor`
- **Log Level**: INFO for normal operation
- **Alert Level**: WARNING for stale assets

### 3. Manual Execution

You can manually trigger the freshness sensor:
```bash
# Using Dagster CLI
dagster sensor run kaizen_wars_freshness_sensor

# Using Dagster UI
# Navigate to Sensors tab and click "Run"
```

## Integration with Existing System

### Compatibility

- ✅ Maintains existing automation sensors
- ✅ Preserves current schedules
- ✅ Compatible with existing DLT pipeline
- ✅ No breaking changes to existing assets

### Dependencies

- `dagster_dlt.DagsterDltResource` - For DLT integration
- `dagster.DagsterInstance` - For asset state queries
- `dagster.FreshnessPolicy` - For freshness evaluation

## Monitoring and Alerting

### Current Capabilities

1. **Real-time Monitoring** - Every minute freshness checks
2. **Visual Feedback** - Color-coded status indicators
3. **Detailed Logging** - Comprehensive status reporting
4. **Error Handling** - Graceful error handling and recovery

### Extensibility

The implementation can be extended to include:
- Email notifications for stale assets
- Slack alerts for critical freshness issues
- Custom remediation actions
- Integration with external monitoring systems

## Troubleshooting

### Common Issues

1. **Sensor Not Running**
   - Check that the sensor is enabled in definitions
   - Verify minimum interval configuration
   - Check Dagster instance connectivity

2. **No Visual Indicators**
   - Ensure freshness policy is configured
   - Verify asset materialization history
   - Check Dagster UI cache

3. **Incorrect Status**
   - Verify freshness policy parameters
   - Check asset materialization timestamps
   - Review sensor evaluation logs

### Debug Commands

```bash
# Check sensor status
dagster sensor list

# Run sensor manually
dagster sensor run kaizen_wars_freshness_sensor

# Check asset history
dagster asset materialization-history dlt_kaizen_wars_fact_virtual

# View freshness policies
dagster asset freshness dlt_kaizen_wars_fact_virtual
```

## Future Enhancements

### Planned Improvements

1. **Alert Integration**
   - Email notifications for stale assets
   - Slack webhook integration
   - Custom alert thresholds

2. **Advanced Monitoring**
   - Historical freshness trends
   - Performance metrics collection
   - Automated remediation

3. **Enhanced UI**
   - Custom dashboard widgets
   - Freshness trend charts
   - Interactive asset status

### Migration Path

The implementation supports gradual migration:
1. Start with basic `FreshnessSensorDefinition`
2. Add enhanced monitoring as needed
3. Implement custom alerting for production
4. Add advanced features for operational excellence

## Conclusion

This freshness sensor implementation provides comprehensive monitoring for the `dlt_kaizen_wars_fact_virtual` asset with minimal configuration overhead. The solution integrates seamlessly with the existing Dagster environment while providing enhanced visibility into data freshness status.

The combination of built-in Dagster freshness evaluation and custom monitoring capabilities ensures robust, reliable, and visually informative freshness tracking for critical data assets.
