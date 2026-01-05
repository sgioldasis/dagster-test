# src/dbt_cloud_orchestration/definitions.py

import dagster as dg
from dbt_cloud_orchestration.defs.dbt_cloud_orchestration import (
    my_dbt_cloud_assets,
    dbt_cloud_polling_sensor,
    workspace,
    dbt_cloud_job_trigger,
    fact_virtual_count_asset,
)

defs = dg.Definitions(
    assets=[my_dbt_cloud_assets, fact_virtual_count_asset],
    sensors=[dbt_cloud_polling_sensor],
    jobs=[dbt_cloud_job_trigger],
    resources={"dbt_cloud": workspace},
)