# src/dbt_cloud_orchestration/defs/ingestion/simple_freshness_sensor.py

import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

import dagster as dg
from dagster import (
    SensorEvaluationContext,
    SensorResult,
    AssetKey,
    AssetSelection,
)


def simple_freshness_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    Simple freshness sensor that provides visual feedback in the lineage view.

    This sensor:
    1. Runs every minute
    2. Provides visual status indicators
    3. Logs freshness information
    4. Shows metadata in the UI
    """

    # Current time for timestamp
    current_time = datetime.now()

    # Simulate freshness check for demonstration
    # In a real implementation, you'd check actual asset materialization times
    import random

    status_options = ["fresh", "stale", "missing"]
    status = random.choice(status_options)

    # Determine visual indicators
    if status == "fresh":
        status_emoji = "✅"
        status_color = "green"
        description = "Fresh - Recently updated"
    elif status == "stale":
        status_emoji = "⚠️"
        status_color = "red"
        description = "Stale - Needs attention"
    else:
        status_emoji = "⚫"
        status_color = "gray"
        description = "No materialization found"

    # Log the status
    context.log.info(
        f"{status_emoji} Freshness Check - Status: {status.upper()}, {description}"
    )

    # Create metadata for visual feedback in the UI
    metadata = {
        "timestamp": current_time.isoformat(),
        "status": status,
        "description": description,
        "status_color": status_color,
        "status_emoji": status_emoji,
        "check_interval": "60 seconds",
        "freshness_policy": "1 minute threshold",
    }

    # Return sensor result with metadata
    return SensorResult(
        run_key=f"freshness_check_{int(current_time.timestamp())}",
        metadata=metadata,
    )


# Create sensor definition
simple_freshness_sensor_def = dg.SensorDefinition(
    name="simple_freshness_sensor",
    evaluation_fn=simple_freshness_sensor,
    minimum_interval_seconds=60,  # Every minute
)
