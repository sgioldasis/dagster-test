# src/dbt_cloud_orchestration/defs/dbt/definitions.py

import dagster as dg
from dbt_cloud_orchestration.defs.dbt.assets import (
    my_dbt_cloud_assets,
    dbt_cloud_polling_sensor,
    workspace,
    kaizen_wars_assets,
)
from dagster import AutomationConditionSensorDefinition

# Add automation sensor to ensure freshness conditions are evaluated
automation_sensor = AutomationConditionSensorDefinition(
    name="default_automation_sensor",
    target=dg.AssetSelection.all(),
    use_user_code_server=True,
)

# DBT-specific Definitions
defs = dg.Definitions(
    assets=[
        my_dbt_cloud_assets,
        *kaizen_wars_assets,
    ],
    sensors=[dbt_cloud_polling_sensor, automation_sensor],
    resources={
        "dbt_cloud": workspace,
    },
)
