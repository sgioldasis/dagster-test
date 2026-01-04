# src/dbt_cloud_orchestration/definitions.py

import dagster as dg
from dbt_cloud_orchestration.defs.dbt_cloud_orchestration import (
    my_dbt_cloud_assets,
    dbt_cloud_polling_sensor,  # Make sure this import exists
    workspace,
)

defs = dg.Definitions(
    assets=[my_dbt_cloud_assets],
    sensors=[dbt_cloud_polling_sensor],  # Make sure this list includes the sensor
    resources={
        "dbt_cloud": workspace,
    },
)