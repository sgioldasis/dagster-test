"""Downstream processing assets for the Kaizen Wars project."""

from downstream.definitions import definitions, downstream_defs
from downstream.defs.fact_virtual_count import fact_virtual_count_asset

__all__ = [
    "definitions",
    "downstream_defs",
    "fact_virtual_count_asset",
]
