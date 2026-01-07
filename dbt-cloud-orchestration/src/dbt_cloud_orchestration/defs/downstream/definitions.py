# src/dbt_cloud_orchestration/defs/downstream/definitions.py

import dagster as dg
from dbt_cloud_orchestration.defs.downstream.fact_virtual_count import fact_virtual_count_asset
from dbt_cloud_orchestration.defs.dbt.assets import workspace

# Downstream-specific Definitions
defs = dg.Definitions(
    assets=[
        fact_virtual_count_asset,
    ],
    resources={
        "dbt_cloud": workspace,
    },
)