import dagster as dg
from datetime import datetime, timedelta

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'customers']))
def customers_freshness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Ensures customers table is updated within 2 minutes and raw_customers is fresh"""
    
    # Retrieve the last materialization event for the customers asset
    target_asset_key = dg.AssetKey(['target', 'main', 'customers'])
    latest_materialization = context.instance.get_latest_materialization_event(
        target_asset_key
    )
    
    if latest_materialization is None:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "info": "Asset has never been materialized."
            }
        )
    
    # Calculate freshness based on the materialization timestamp
    last_update_timestamp = latest_materialization.timestamp
    last_update = datetime.fromtimestamp(last_update_timestamp)
    current_time = datetime.now()
    
    lag = current_time - last_update
    threshold_minutes = 2
    is_fresh = lag < timedelta(minutes=threshold_minutes)
    
    # Check if raw_customers is overdue
    raw_customers_key = dg.AssetKey(['target', 'main', 'raw_customers'])
    raw_customers_materialization = context.instance.get_latest_materialization_event(
        raw_customers_key
    )
    
    if raw_customers_materialization is None:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "info": "raw_customers has never been materialized."
            }
        )
    
    raw_customers_last_update_timestamp = raw_customers_materialization.timestamp
    raw_customers_last_update = datetime.fromtimestamp(raw_customers_last_update_timestamp)
    raw_customers_lag = current_time - raw_customers_last_update
    raw_customers_is_fresh = raw_customers_lag < timedelta(minutes=threshold_minutes)
    
    # The customers asset is overdue if either customers or raw_customers is overdue
    overall_is_fresh = is_fresh and raw_customers_is_fresh
    
    return dg.AssetCheckResult(
        passed=overall_is_fresh,
        metadata={
            "dagster/freshness_threshold_minutes": threshold_minutes,
            "last_update": last_update.isoformat(),
            "lag_minutes": lag.total_seconds() / 60,
            "raw_customers_last_update": raw_customers_last_update.isoformat(),
            "raw_customers_lag_minutes": raw_customers_lag.total_seconds() / 60,
            "info": f"Last materialization was {lag.total_seconds() / 60:.1f} minutes ago. raw_customers was updated {raw_customers_lag.total_seconds() / 60:.1f} minutes ago."
        }
    )
