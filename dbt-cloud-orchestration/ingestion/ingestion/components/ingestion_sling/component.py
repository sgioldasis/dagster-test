"""Custom Sling Component for data ingestion.

This module provides a custom Dagster component that extends SlingReplicationCollectionComponent
to add project-specific behavior for data replication using Sling.

What is Sling?
    Sling is a CLI tool for moving data between databases and file systems.
    It supports sources like CSV, PostgreSQL, MySQL, and targets like
    PostgreSQL, Databricks, Snowflake, BigQuery, etc.

Component Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │           IngestionSlingComponent                            │
    │                     (this file)                              │
    └─────────────────────────┬───────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   ┌─────────┐         ┌─────────────┐      ┌──────────────┐
   │Sling    │         │Custom       │      │Environment   │
   │Resource │◄────────│Translator   │      │Variables     │
   └─────────┘         └─────────────┘      └──────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ Asset Specs     │
                    │ - Custom keys   │
                    │ - Freshness     │
                    │ - Dependencies  │
                    └─────────────────┘

Key Features:
    1. Custom Asset Key Mapping: Removes 'target' prefix from asset keys
    2. Upstream Dependencies: Reads deps from YAML metadata
    3. Freshness Policies: Configured per-asset for data quality
    4. Environment Variable Resolution: Supports env: and ${} syntax
    5. Databricks HTTP Path: Auto-constructs from warehouse_id

Replication Configuration:
    Replication specs are defined in YAML files (replication_csv.yaml,
    replication_db.yaml) which specify source/target connections and streams.
"""

import os
from functools import cached_property
from pathlib import Path
from typing import Any, Mapping, Sequence
from datetime import timedelta

import dagster as dg
from dagster import AssetDep, AssetKey, AssetSpec
from dagster_sling import (
    DagsterSlingTranslator,
    SlingConnectionResource,
    SlingReplicationCollectionComponent,
    SlingResource,
)
from dotenv import load_dotenv

# Load environment variables at module load time
load_dotenv()


class IngestionSlingTranslator(DagsterSlingTranslator):
    """Custom translator for Sling stream definitions to Dagster asset specs.

    This translator customizes how Sling replication streams are represented
    as Dagster assets. It provides:

    1. Asset Key Customization: Strips 'target' prefix from paths for cleaner keys
    2. Dependency Management: Reads upstream asset dependencies from YAML metadata
    3. Freshness Policies: Applies time-window freshness policies per asset
    4. Asset Kinds: Tags assets with source/target system types
    5. Source Asset Control: Optionally hides file-based source assets

    The translator is used by IngestionSlingComponent when building asset specs
    from Sling replication configurations.
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
        # Return empty list to hide source assets
        # This prevents showing file path-based assets in the Dagster UI
        return []

    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> AssetSpec:
        """Convert a Sling stream definition to a Dagster AssetSpec.

        This method is called for each stream in the replication configuration
        to determine how it should appear in Dagster's asset graph.

        Args:
            stream_definition: Dictionary containing stream configuration from YAML.
                Includes keys like 'object', 'mode', 'meta', etc.

        Returns:
            AssetSpec with customized key, dependencies, freshness policy, and tags.

        Example stream_definition:
            {
                'object': 'public.fact_virtual',
                'meta': {
                    'dagster': {
                        'asset_key': 'databricks_fact_virtual',
                        'upstream_assets': ['csv_fact_virtual'],
                        'group': 'ingestion'
                    }
                }
            }
        """
        # Get the base spec from the parent translator
        spec = super().get_asset_spec(stream_definition)

        # Strip 'target' prefix from asset key path if present
        # This gives us cleaner asset keys like "csv_fact_virtual"
        # instead of "target/csv_fact_virtual"
        path = list(spec.key.path)
        if path and path[0] == "target":
            path = path[1:]

        # Read upstream dependencies from YAML metadata
        # This allows defining asset dependencies in the replication YAML
        deps = list(spec.deps)
        stream_config = stream_definition.get("config", {})
        dagster_meta = stream_config.get("meta", {}).get("dagster", {})

        if "upstream_assets" in dagster_meta:
            # Convert upstream asset strings to AssetDep objects
            deps = [
                AssetDep(AssetKey(asset)) for asset in dagster_meta["upstream_assets"]
            ]

        # Configure asset-specific metadata based on the asset key
        freshness_policy = None
        kinds = None

        if spec.key.to_string() == '["csv_fact_virtual"]':
            # CSV → PostgreSQL ingestion asset
            # Short freshness window since CSV files are expected to update frequently
            freshness_policy = dg.FreshnessPolicy.time_window(
                fail_window=timedelta(minutes=2),
                warn_window=timedelta(minutes=1),  # Warn before failing
            )
            kinds = {"sling", "csv", "postgres"}  # Tags for UI visualization

        elif spec.key.to_string() == '["databricks_fact_virtual"]':
            # PostgreSQL → Databricks ingestion asset
            # Longer window since this involves network transfer to Databricks
            freshness_policy = dg.FreshnessPolicy.time_window(
                fail_window=timedelta(minutes=5),
                warn_window=timedelta(minutes=2),
            )
            kinds = {"sling", "postgres", "databricks"}

        # Build the asset spec attributes
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


class IngestionSlingComponent(SlingReplicationCollectionComponent):
    """Custom Sling component for the ingestion project.

    This component extends SlingReplicationCollectionComponent to provide:
    1. Custom translator for asset spec generation
    2. Environment variable resolution in connection strings
    3. Databricks-specific connection handling

    Configuration (in component.yaml):
        type: ingestion.IngestionSlingComponent
        attributes:
          connections:
            CSV_SOURCE:          # Connection name referenced in replication YAML
              type: file
            POSTGRES_DEST:
              type: postgres
              connection_string: env:POSTGRES_CONNECTION_STRING
            DATABRICKS_TARGET:
              type: databricks
              host: env:DATABRICKS_HOST
              token: env:DATABRICKS_TOKEN
              warehouse_id: env:DATABRICKS_WAREHOUSE_ID
          replications:
            - path: replication_csv.yaml    # Relative to component directory
            - path: replication_db.yaml

    The component discovers these YAML files and creates Dagster assets
    for each stream defined within them.
    """

    # Connection names based on component.yaml order
    # These must match the order in component.yaml
    _CONNECTION_NAMES: dict[int, str] = {
        0: "CSV_SOURCE",
        1: "POSTGRES_DEST",
        2: "POSTGRES_TARGET",
        3: "DATABRICKS_TARGET",
    }


    @cached_property
    def _base_translator(self) -> DagsterSlingTranslator:
        """Create and cache the custom translator instance.

        Returns:
            IngestionSlingTranslator for converting Sling streams to asset specs.
        """
        return IngestionSlingTranslator()

    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> AssetSpec:
        """Get the asset spec for a stream (delegates to translator).

        Args:
            stream_definition: Stream configuration from replication YAML.

        Returns:
            Customized AssetSpec for this stream.
        """
        return self._base_translator.get_asset_spec(stream_definition)

    @cached_property
    def sling_resource(self) -> SlingResource:
        """Build the SlingResource with resolved connections.

        This method processes connection configurations from the YAML and:
        1. Resolves environment variables (env:VAR_NAME or ${VAR_NAME} syntax)
        2. Auto-constructs Databricks http_path from warehouse_id if needed
        3. Creates SlingConnectionResource objects for each connection

        Environment Variable Syntax:
            - env:POSTGRES_CONNECTION_STRING  # Sling-style
            - ${DATABRICKS_HOST}              # Shell-style

        Returns:
            Configured SlingResource ready for replication execution.
        """
        # self.connections is a Sequence of SlingConnectionResource
        # We need to preserve the connection names from the original config

        resolved_connections = []
        for idx, conn in enumerate(self.connections):
            conn_dict = conn.model_dump()

            # Set the connection name from our predefined mapping
            if idx in self._CONNECTION_NAMES:
                conn_dict["name"] = self._CONNECTION_NAMES[idx]
            elif hasattr(conn, 'name') and conn.name:
                conn_dict["name"] = conn.name

            # Resolve environment variables in connection properties
            for key, value in conn_dict.items():
                if isinstance(value, str):
                    if value.startswith("env:"):
                        # Sling-style env var reference: env:VAR_NAME
                        conn_dict[key] = os.environ.get(value[4:], "")
                    elif value.startswith("${") and value.endswith("}"):
                        # Shell-style env var reference: ${VAR_NAME}
                        conn_dict[key] = os.environ.get(value[2:-1], "")

            # Special handling for CSV_SOURCE file connection
            # Set the URL from environment variable if not already set
            if conn_dict.get("name") == "CSV_SOURCE" and conn_dict.get("type") == "file":
                csv_path = os.environ.get("CSV_DATA_PATH", "")
                if csv_path:
                    # Ensure file:// protocol prefix
                    if not csv_path.startswith("file://"):
                        csv_path = f"file://{csv_path}"
                    conn_dict["url"] = csv_path

            # Auto-construct Databricks http_path if warehouse_id is provided
            if conn_dict.get("type") == "databricks":
                if not conn_dict.get("http_path") and conn_dict.get("warehouse_id"):
                    conn_dict["http_path"] = (
                        f"/sql/1.0/warehouses/{conn_dict['warehouse_id']}"
                    )

            resolved_connections.append(SlingConnectionResource(**conn_dict))

        return SlingResource(connections=resolved_connections)

    def execute(
        self,
        context: dg.AssetExecutionContext,
        sling: SlingResource,
        replication_spec_model: Any,
    ) -> Any:
        """Execute a Sling replication for the given specification.

        This method is called by Dagster when the asset is materialized.
        It runs the Sling replication using the configuration from the
        specified YAML file.

        Args:
            context: Asset execution context for logging and metadata.
            sling: The SlingResource (passed by Dagster's resource system).
            replication_spec_model: Model containing the path to the replication YAML.

        Yields:
            Asset materialization events and metadata from the replication.

        Example replication_spec_model:
            ReplicationSpecModel(path="replication_csv.yaml")
        """
        config_path = Path(__file__).parent / replication_spec_model.path
        context.log.info(f"Executing Sling replication for {config_path}")

        # Use the custom sling_resource with resolved env vars
        # The passed 'sling' uses base class sling_resource which doesn't resolve env vars
        yield from self.sling_resource.replicate(
            context=context,
            replication_config=config_path,
            dagster_sling_translator=self._base_translator,
        )
