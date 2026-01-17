# src/dbt_cloud_orchestration/defs/downstream/definitions.py
"""
Downstream code location - Independent processing assets.

This code location is independent but depends on:
- stg_kaizen_wars__fact_virtual (from dbt code location)

Cross-location dependencies are declared via SourceAsset to avoid
coupling between code locations.
"""

import dagster as dg
from dagster import EnvVar, SourceAsset

from dbt_cloud_orchestration.defs.downstream.fact_virtual_count import (
    fact_virtual_count_asset,
)

stg_kaizen_wars_fact_virtual_source = SourceAsset(
    key="stg_kaizen_wars__fact_virtual",
    description="dbt staging asset from dbt code location",
)


def downstream_defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            fact_virtual_count_asset,
            stg_kaizen_wars_fact_virtual_source,
        ],
    )
