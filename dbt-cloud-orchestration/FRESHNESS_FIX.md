## DLT Freshness UI Fix Summary

### ✅ **Issues Fixed**

1. **Missing Data Files** - Created required CSV files in `data/` directory:
   - `raw_customers.csv`
   - `raw_orders.csv` 
   - `raw_payments.csv`
   - `raw_fact_virtual.csv`

2. **Missing Automation Sensor** - Added DLT-specific automation sensor in `ingestion/definitions.py`:
   ```python
   dlt_automation_sensor = AutomationConditionSensorDefinition(
       name="dlt_automation_sensor",
       target=dg.AssetSelection.keys(
           "dlt_csv_data_source_customers",
           "dlt_csv_data_source_orders", 
           "dlt_csv_data_source_payments",
           "dlt_kaizen_wars_fact_virtual"
       ),
       use_user_code_server=True,
   )
   ```

3. **Added Freshness Schedule** - Created schedule to ensure regular materialization:
   ```python
   dlt_freshness_schedule = dg.ScheduleDefinition(
       name="dlt_freshness_schedule",
       target=dg.AssetSelection.keys(...),
       cron_schedule="*/2 * * * *",  # Every 2 minutes
   )
   ```

4. **Cleaned Up Configuration** - Removed redundant freshness policy function

### ✅ **Current Configuration**

- **Ingestion Location**: Has 2 assets, 2 schedules, 1 automation sensor
- **DBT Location**: Has dbt assets + automation sensor for overall automation
- **All DLT Assets**: Have freshness policies (1-minute tolerance)
- **All DLT Assets**: Have eager automation conditions

### ✅ **Expected Behavior**

1. **First Run**: DLT assets will materialize from CSV files
2. **Freshness Check**: Every minute, Dagster checks if assets are fresh
3. **Overdue Status**: After 2 minutes without materialization, assets will show "Overdue" in UI
4. **Auto-Trigger**: Automation sensor will trigger runs when assets become overdue

### 🔧 **Next Steps**

1. **Start Dagster**: `dg dev --workspace workspace.yaml`
2. **Monitor UI**: After 2 minutes of inactivity, DLT assets should show "Overdue" indicator
3. **Test Automation**: Verify that overdue assets trigger runs automatically

The "Overdue" indicator should now appear in the Dagster UI lineage within 2 minutes of the last materialization.