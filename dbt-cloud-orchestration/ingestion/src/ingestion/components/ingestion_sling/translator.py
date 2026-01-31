"""Custom translator for Sling stream definitions to Dagster asset specs.

This module provides IngestionSlingTranslator which customizes how Sling
replication streams are represented as Dagster assets.
"""

from datetime import timedelta
from typing import Any, Mapping

import dagster as dg
from dagster import AssetDep, AssetKey, AssetSpec
from dagster_sling import DagsterSlingTranslator


class IngestionSlingTranslator(DagsterSlingTranslator):
    """Custom translator for Sling stream definitions to Dagster asset specs.

    This translator customizes how Sling replication streams are represented
    as Dagster assets. It provides:

    1. Asset Key Customization: Strips 'target' prefix from paths for cleaner keys
    2. Dependency Management: Reads upstream asset dependencies from YAML metadata
    3. Freshness Policies: Applies time-window freshness policies per asset
    4. Asset Kinds: Tags assets with source/target system types
    5. Source Asset Control: Optionally hides file-based source assets
    """

    def get_deps(self, stream_definition: Mapping[str, Any]) -> list[AssetDep]:
        """Get upstream dependencies for a stream.

        Overrides the default behavior to hide file-based source assets
        by returning an empty list. This prevents Dagster from showing
        source assets that expose local file paths.

        Args:
            stream_definition: Stream configuration from replication YAML.

        Returns:
            Empty list to hide source assets.
        """
        return []

    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> AssetSpec:
        """Convert a Sling stream definition to a Dagster AssetSpec.

        This method is called for each stream in the replication configuration
        to determine how it should appear in Dagster's asset graph.

        Args:
            stream_definition: Dictionary containing stream configuration from YAML.

        Returns:
            AssetSpec with customized key, dependencies, freshness policy, and tags.
        """
        spec = super().get_asset_spec(stream_definition)

        # Strip 'target' prefix from asset key path if present
        path = list(spec.key.path)
        if path and path[0] == "target":
            path = path[1:]

        # Read upstream dependencies from YAML metadata
        deps: list[AssetDep] = []
        stream_config = stream_definition.get("config", {})
        dagster_meta = stream_config.get("meta", {}).get("dagster", {})

        if "upstream_assets" in dagster_meta:
            deps = [
                AssetDep(AssetKey(asset)) for asset in dagster_meta["upstream_assets"]
            ]

        # Configure asset-specific metadata
        freshness_policy = self._get_freshness_policy(spec.key)
        kinds = self._get_kinds(spec.key)

        spec_attrs: dict[str, Any] = {
            "key": dg.AssetKey(path),
            "deps": deps,
            "group_name": "ingestion",
            "automation_condition": dg.AutomationCondition.eager(),
            "tags": {"freshness_evaluated": "true"},
        }

        if freshness_policy:
            spec_attrs["freshness_policy"] = freshness_policy

        if kinds:
            spec_attrs["kinds"] = kinds

        return spec.replace_attributes(**spec_attrs)

    def _get_freshness_policy(self, key: AssetKey) -> dg.FreshnessPolicy | None:
        """Get freshness policy for a given asset key."""
        asset_key_str = key.to_string()

        if asset_key_str == '["csv_fact_virtual"]':
            return dg.FreshnessPolicy.time_window(
                fail_window=timedelta(minutes=2),
                warn_window=timedelta(minutes=1),
            )
        elif asset_key_str == '["fact_virtual"]':
            return dg.FreshnessPolicy.time_window(
                fail_window=timedelta(minutes=5),
                warn_window=timedelta(minutes=2),
            )
        return None

    def _get_kinds(self, key: AssetKey) -> set[str] | None:
        """Get asset kinds for a given asset key."""
        asset_key_str = key.to_string()

        if asset_key_str == '["csv_fact_virtual"]':
            return {"sling", "csv", "postgres"}
        elif asset_key_str == '["fact_virtual"]':
            return {"sling", "postgres", "databricks"}
        return None
