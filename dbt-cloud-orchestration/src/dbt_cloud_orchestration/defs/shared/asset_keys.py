# src/dbt_cloud_orchestration/defs/shared/asset_keys.py
"""Cross-location asset key definitions.

These constants are used to declare dependencies between code locations
without creating import coupling. Each code location can reference these
keys to declare SourceAssets for external dependencies.
"""

from dagster import AssetKey


class IngestionAssetKeys:
    """Asset keys for the ingestion code location."""

    DLT_KAIZEN_WARS_FACT_VIRTUAL = AssetKey("dlt_kaizen_wars_fact_virtual")


class DbtAssetKeys:
    """Asset keys for the dbt code location."""

    STG_KAIZEN_WARS_FACT_VIRTUAL = AssetKey("stg_kaizen_wars__fact_virtual")


class DownstreamAssetKeys:
    """Asset keys for the downstream code location."""

    FACT_VIRTUAL_COUNT = AssetKey("fact_virtual_count")


__all__ = [
    "IngestionAssetKeys",
    "DbtAssetKeys",
    "DownstreamAssetKeys",
]
