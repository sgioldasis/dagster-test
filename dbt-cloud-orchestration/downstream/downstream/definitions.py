"""Downstream code location - Independent processing assets."""

import dagster as dg
from dagster import EnvVar, SourceAsset
from dotenv import load_dotenv

load_dotenv()

from .defs.fact_virtual_count import (
    fact_virtual_count_asset,
)

stg_kaizen_wars_fact_virtual_source = SourceAsset(
    key="stg_kaizen_wars__fact_virtual",
    description="dbt staging asset from dbt code location",
    group_name="dbt",
)


defs = dg.Definitions(
    assets=[
        fact_virtual_count_asset,
        stg_kaizen_wars_fact_virtual_source,
    ],
)


def downstream_defs() -> dg.Definitions:
    """Wrapper function for tools that expect a function attribute."""
    return defs
