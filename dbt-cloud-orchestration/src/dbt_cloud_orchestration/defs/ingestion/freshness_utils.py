# src/dbt_cloud_orchestration/defs/ingestion/freshness_utils.py

import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import dagster as dg
from dagster import AssetKey, FreshnessPolicy, FreshnessPolicyEvaluationData


class FreshnessMonitor:
    """
    Utility class for monitoring asset freshness with enhanced visual feedback.
    """
    
    def __init__(self, instance: dg.DagsterInstance):
        self.instance = instance
    
    def get_freshness_status(self, asset_key: AssetKey) -> Dict[str, Any]:
        """
        Get comprehensive freshness status for an asset.
        
        Returns a dictionary with:
        - status: 'fresh', 'stale', 'missing', 'unknown'
        - description: Human-readable description with emoji
        - color: Color for visual indication (green, red, yellow, gray)
        - last_materialization: Last materialization time
        - time_since_last: Time since last materialization
        - freshness_policy: Applied freshness policy
        - threshold: Freshness threshold
        """
        
        try:
            # Get asset state and spec
            asset_state = self.instance.get_asset_state(asset_key=asset_key)
            asset_spec = self.instance.get_asset_spec(asset_key=asset_key)
            
            if not asset_state or not asset_spec:
                return {
                    "status": "unknown",
                    "description": "❓ Asset or spec not found",
                    "color": "gray",
                    "last_materialization": None,
                    "time_since_last": None,
                    "freshness_policy": None,
                    "threshold": None,
                }
            
            # Get freshness policy
            freshness_policy = asset_spec.freshness_policy
            if not freshness_policy:
                return {
                    "status": "unknown",
                    "description": "❓ No freshness policy configured",
                    "color": "gray",
                    "last_materialization": asset_state.last_materialization_time,
                    "time_since_last": None,
                    "freshness_policy": None,
                    "threshold": None,
                }
            
            # Calculate freshness evaluation
            current_time = datetime.now()
            last_materialization = asset_state.last_materialization_time
            
            if last_materialization is None:
                return {
                    "status": "missing",
                    "description": "❌ No materialization found",
                    "color": "gray",
                    "last_materialization": None,
                    "time_since_last": None,
                    "freshness_policy": freshness_policy,
                    "threshold": freshness_policy.lower_bound_delta,
                }
            
            # Calculate time since last materialization
            time_since_last = current_time - last_materialization
            lower_bound_delta = freshness_policy.lower_bound_delta or timedelta(minutes=0)
            
            # Determine freshness status
            if time_since_last <= lower_bound_delta:
                status = "fresh"
                emoji = "✅"
                color = "green"
            else:
                status = "stale"
                emoji = "⚠️"
                color = "red"
            
            # Create description
            description = f"{emoji} {status.title()} - Updated {time_since_last.total_seconds():.0f}s ago (threshold: {lower_bound_delta.total_seconds():.0f}s)"
            
            return {
                "status": status,
                "description": description,
                "color": color,
                "last_materialization": last_materialization,
                "time_since_last": time_since_last,
                "freshness_policy": freshness_policy,
                "threshold": lower_bound_delta,
            }
            
        except Exception as e:
            return {
                "status": "error",
                "description": f"❌ Error checking freshness: {str(e)}",
                "color": "red",
                "last_materialization": None,
                "time_since_last": None,
                "freshness_policy": None,
                "threshold": None,
            }
    
    def get_freshness_summary(self, asset_keys: list[AssetKey]) -> Dict[str, Any]:
        """
        Get a summary of freshness status for multiple assets.
        
        Returns a summary with counts and overall status.
        """
        
        summary = {
            "total_assets": len(asset_keys),
            "fresh_count": 0,
            "stale_count": 0,
            "missing_count": 0,
            "unknown_count": 0,
            "error_count": 0,
            "assets": {},
            "overall_status": "unknown",
        }
        
        for asset_key in asset_keys:
            status_info = self.get_freshness_status(asset_key)
            summary["assets"][str(asset_key)] = status_info
            
            # Count by status
            status = status_info["status"]
            if status in summary:
                summary[f"{status}_count"] += 1
        
        # Determine overall status
        if summary["stale_count"] > 0:
            summary["overall_status"] = "stale"
        elif summary["fresh_count"] == summary["total_assets"] and summary["total_assets"] > 0:
            summary["overall_status"] = "fresh"
        elif summary["missing_count"] == summary["total_assets"] and summary["total_assets"] > 0:
            summary["overall_status"] = "missing"
        else:
            summary["overall_status"] = "mixed"
        
        return summary


def create_freshness_asset_key(asset_name: str, group_name: str = "ingestion") -> AssetKey:
    """
    Create an asset key for freshness monitoring.
    """
    return AssetKey([group_name, asset_name])


def get_freshness_threshold(freshness_policy: FreshnessPolicy) -> timedelta:
    """
    Get the effective freshness threshold from a policy.
    """
    return freshness_policy.lower_bound_delta or timedelta(minutes=0)


def format_freshness_duration(duration: timedelta) -> str:
    """
    Format a timedelta duration for human readability.
    """
    if duration.total_seconds() < 60:
        return f"{duration.total_seconds():.0f}s"
    elif duration.total_seconds() < 3600:
        return f"{duration.total_seconds() / 60:.1f}m"
    else:
        return f"{duration.total_seconds() / 3600:.1f}h"
