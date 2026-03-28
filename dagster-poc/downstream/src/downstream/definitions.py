"""Downstream code location - Independent processing assets."""

import dagster as dg
from dotenv import load_dotenv

from downstream.defs.fact_virtual_count import fact_virtual_count_asset

# Load environment variables from .env file
load_dotenv()


definitions = dg.Definitions(
    assets=[
        fact_virtual_count_asset,
    ],
)


def downstream_defs() -> dg.Definitions:
    """Wrapper function for tools that expect a function attribute."""
    return definitions
