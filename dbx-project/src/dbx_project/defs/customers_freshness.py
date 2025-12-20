import dagster as dg
from datetime import datetime, timedelta

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'customers']))
def customers_freshness(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Ensures customers table is updated within 2 minutes"""
    
    # Retrieve the last materialization event for the asset
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
    
    return dg.AssetCheckResult(
        passed=is_fresh,
        metadata={
            "dagster/freshness_threshold_minutes": threshold_minutes,
            "last_update": last_update.isoformat(),
            "lag_minutes": lag.total_seconds() / 60,
            "info": f"Last materialization was {lag.total_seconds() / 60:.1f} minutes ago."
        }
    )
