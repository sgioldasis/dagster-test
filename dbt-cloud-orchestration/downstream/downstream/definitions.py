"""Downstream code location - Independent processing assets."""

import dagster as dg
from dagster import EnvVar
from dotenv import load_dotenv

load_dotenv()

from .defs.fact_virtual_count import (
    fact_virtual_count_asset,
)


defs = dg.Definitions(
    assets=[
        fact_virtual_count_asset,
    ],
)


def downstream_defs() -> dg.Definitions:
    """Wrapper function for tools that expect a function attribute."""
    return defs
