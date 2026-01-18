"""Downstream code location - Independent processing assets."""

import dagster as dg
from dagster import EnvVar, SourceAsset

from .fact_virtual_count import (
    fact_virtual_count_asset,
)

stg_kaizen_wars_fact_virtual_source = SourceAsset(
    key="stg_kaizen_wars__fact_virtual",
    description="dbt staging asset from dbt code location",
    group_name="dbt",
)


def downstream_defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            fact_virtual_count_asset,
            stg_kaizen_wars_fact_virtual_source,
        ],
    )
