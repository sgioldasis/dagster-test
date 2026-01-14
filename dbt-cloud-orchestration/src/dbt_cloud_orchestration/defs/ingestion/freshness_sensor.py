# src/dbt_cloud_orchestration/defs/ingestion/freshness_sensor.py

import os
import time
from datetime import datetime, timedelta
from typing import Optional

import dagster as dg
from dagster import (
    SensorEvaluationContext,
    SensorResult,
    RunRequest,
    SkipReason,
    AssetKey,
    FreshnessPolicy,
    FreshnessSensorResult,
    AssetSelection,
)
from dagster._core.definitions.freshness_policy import FreshnessPolicyEvaluationData


def kaizen_wars_freshness_sensor(context: SensorEvaluationContext) -> FreshnessSensorResult:
    """
    Freshness sensor for the dlt_kaizen_wars_fact_virtual asset.
    
    This sensor evaluates the freshness status of the Kaizen Wars fact_virtual asset
    every minute and provides visual indication in the lineage view.
    
    The sensor checks:
    1. Asset materialization status
    2. Freshness compliance 
    3. Provides visual feedback through Dagster's UI
    """
    
    target_asset_key = AssetKey(["dlt_kaizen_wars_fact_virtual"])
    
    try:
        # Get the current state of the target asset
        asset_state = context.instance.get_asset_state(asset_key=target_asset_key)
        
        if not asset_state:
            context.log.info(f"Asset {target_asset_key} not found in Dagster instance")
            return FreshnessSensorResult(
                asset_key=target_asset_key,
                current_time=datetime.now(),
                last_materialization_time=None,
                freshness_evaluation_data=None,
                evaluation_time=datetime.now(),
                reason="Asset not found in Dagster instance",
                skip_reason=SkipReason("Asset not found"),
            )
        
        # Get the last materialization time
        last_materialization_time = asset_state.last_materialization_time
        
        # Get the freshness policy for this asset
        asset_spec = context.instance.get_asset_spec(asset_key=target_asset_key)
        freshness_policy = asset_spec.freshness_policy
        
        if not freshness_policy:
            context.log.warning(f"No freshness policy found for asset {target_asset_key}")
            return FreshnessSensorResult(
                asset_key=target_asset_key,
                current_time=datetime.now(),
                last_materialization_time=last_materialization_time,
                freshness_evaluation_data=None,
                evaluation_time=datetime.now(),
                reason="No freshness policy configured",
                skip_reason=SkipReason("No freshness policy configured"),
            )
        
        # Calculate freshness evaluation data
        freshness_evaluation_data = FreshnessPolicyEvaluationData(
            freshness_policy=freshness_policy,
            current_time=datetime.now(),
            last_materialization_time=last_materialization_time,
        )
        
        # Evaluate freshness status
        is_fresh = freshness_evaluation_data.is_fresh()
        is_stale = freshness_evaluation_data.is_stale()
        
        # Determine the visual status for lineage view
        if is_fresh:
            status = "fresh"
            status_color = "green"
            description = f"✅ Fresh (updated {freshness_evaluation_data.time_since_last_materialization.total_seconds():.0f}s ago)"
        elif is_stale:
            status = "stale"
            status_color = "red"
            description = f"⚠️ Stale (last updated {freshness_evaluation_data.time_since_last_materialization.total_seconds():.0f}s ago)"
        else:
            status = "unknown"
            status_color = "yellow"
            description = f"⏳ Unknown (last updated {freshness_evaluation_data.time_since_last_materialization.total_seconds():.0f}s ago)"
        
        # Log the freshness status
        context.log.info(
            f"Kaizen Wars freshness check - Asset: {target_asset_key}, "
            f"Status: {status}, "
            f"Last materialization: {last_materialization_time}, "
            f"Freshness: {description}"
        )
        
        # Return the freshness sensor result with visual indication
        return FreshnessSensorResult(
            asset_key=target_asset_key,
            current_time=datetime.now(),
            last_materialization_time=last_materialization_time,
            freshness_evaluation_data=freshness_evaluation_data,
            evaluation_time=datetime.now(),
            reason=description,
            # The freshness status will be automatically displayed in the UI
        )
        
    except Exception as e:
        context.log.error(f"Error evaluating freshness for {target_asset_key}: {str(e)}")
        return FreshnessSensorResult(
            asset_key=target_asset_key,
            current_time=datetime.now(),
            last_materialization_time=None,
            freshness_evaluation_data=None,
            evaluation_time=datetime.now(),
            reason=f"Error evaluating freshness: {str(e)}",
            skip_reason=SkipReason(f"Error: {str(e)}"),
        )


# Create the sensor definition
kaizen_wars_freshness_sensor_def = dg.FreshnessSensorDefinition(
    name="kaizen_wars_freshness_sensor",
    monitorable_asset_selection=AssetSelection.keys("dlt_kaizen_wars_fact_virtual"),
    evaluation_fn=kaizen_wars_freshness_sensor,
    minimum_interval_seconds=60,  # Evaluate every minute
)


# Alternative implementation using a regular sensor for more control
def kaizen_wars_freshness_sensor_legacy(context: SensorEvaluationContext) -> SensorResult:
    """
    Alternative freshness sensor implementation using regular sensor for more control.
    This provides additional visual feedback and custom logic.
    """
    
    target_asset_key = AssetKey(["dlt_kaizen_wars_fact_virtual"])
    
    try:
        # Get asset state and freshness policy
        asset_state = context.instance.get_asset_state(asset_key=target_asset_key)
        asset_spec = context.instance.get_asset_spec(asset_key=target_asset_key)
        freshness_policy = asset_spec.freshness_policy
        
        if not asset_state or not freshness_policy:
            return SkipReason(f"Asset or freshness policy not available for {target_asset_key}")
        
        last_materialization_time = asset_state.last_materialization_time
        current_time = datetime.now()
        
        # Calculate freshness status
        time_since_last_materialization = current_time - last_materialization_time if last_materialization_time else timedelta.max
        lower_bound_delta = freshness_policy.lower_bound_delta or timedelta(minutes=0)
        
        # Determine if asset is fresh, stale, or missing
        if last_materialization_time is None:
            status = "missing"
            status_emoji = "❌"
            description = "No materialization found"
        elif time_since_last_materialization <= lower_bound_delta:
            status = "fresh"
            status_emoji = "✅"
            description = f"Fresh (updated {time_since_last_materialization.total_seconds():.0f}s ago)"
        else:
            status = "stale"
            status_emoji = "⚠️"
            description = f"Stale (updated {time_since_last_materialization.total_seconds():.0f}s ago)"
        
        # Log the status with visual indicators
        context.log.info(
            f"{status_emoji} Kaizen Wars Freshness - Asset: {target_asset_key}, "
            f"Status: {status.upper()}, "
            f"Time since last update: {time_since_last_materialization.total_seconds():.0f}s, "
            f"Threshold: {lower_bound_delta.total_seconds():.0f}s"
        )
        
        # Provide visual feedback through Dagster's metadata system
        run_config = {
            "ops": {
                "log_freshness_status": {
                    "config": {
                        "asset_key": str(target_asset_key),
                        "status": status,
                        "description": description,
                        "last_materialization": last_materialization_time.isoformat() if last_materialization_time else None,
                        "current_time": current_time.isoformat(),
                    }
                }
            }
        }
        
        # Return sensor result (no run request needed for monitoring only)
        return SensorResult(
            run_key=f"freshness_check_{int(time.time())}",
            run_config=run_config,
            metadata={
                "asset_key": str(target_asset_key),
                "status": status,
                "description": description,
                "last_materialization": last_materialization_time,
                "freshness_policy": str(freshness_policy),
            }
        )
        
    except Exception as e:
        context.log.error(f"Error in Kaizen Wars freshness sensor: {str(e)}")
        return SkipReason(f"Error: {str(e)}")


# Legacy sensor definition (commented out as FreshnessSensorDefinition is preferred)
# kaizen_wars_legacy_sensor = dg.SensorDefinition(
#     name="kaizen_wars_freshness_sensor_legacy",
#     evaluation_fn=kaizen_wars_freshness_sensor_legacy,
#     minimum_interval_seconds=60,
# )
