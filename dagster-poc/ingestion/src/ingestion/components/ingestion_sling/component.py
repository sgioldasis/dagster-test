"""Custom Sling Component for data ingestion.

This module provides a custom Dagster component that extends SlingReplicationCollectionComponent
to add project-specific behavior for data replication using Sling.
"""

import os
from functools import cached_property
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetsDefinition
from dagster_sling import (
    SlingConnectionResource,
    SlingReplicationCollectionComponent,
    SlingResource,
)

from ingestion.components.ingestion_sling.config_resolver import process_replication_config
from ingestion.components.ingestion_sling.translator import IngestionSlingTranslator


class IngestionSlingComponent(SlingReplicationCollectionComponent):
    """Custom Sling component for the ingestion project.

    This component extends SlingReplicationCollectionComponent to provide:
    1. Custom translator for asset spec generation
    2. Environment variable resolution in connection strings
    3. Databricks-specific connection handling
    """

    # Connection names based on component.yaml order
    _CONNECTION_NAMES: dict[int, str] = {
        0: "CSV_SOURCE",
        1: "POSTGRES_DEST",
        2: "POSTGRES_TARGET",
        3: "DATABRICKS_TARGET",
    }

    def build_asset(
        self,
        context: dg.ComponentLoadContext,
        replication_spec_model: Any,
    ) -> AssetsDefinition:
        """Build a Sling asset using the custom translator."""
        from dagster import AssetExecutionContext
        from dagster_sling import sling_assets

        op_spec = replication_spec_model.op

        # Process the config to resolve env vars before passing to sling_assets
        original_config_path = context.path / replication_spec_model.path
        processed_config_path = process_replication_config(original_config_path)

        @sling_assets(
            name=op_spec.name if op_spec and op_spec.name else Path(replication_spec_model.path).stem,
            op_tags=op_spec.tags if op_spec else None,
            replication_config=processed_config_path,
            dagster_sling_translator=self._base_translator,
            backfill_policy=op_spec.backfill_policy if op_spec else None,
        )
        def _asset(context: AssetExecutionContext, sling: SlingResource):
            yield from self.execute(
                context=context,
                sling=sling,
                replication_spec_model=replication_spec_model,
            )

        return _asset

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions with sling resource."""
        return dg.Definitions(
            assets=[self.build_asset(context, replication) for replication in self.replications],
            resources={"sling": self.sling_resource},
        )

    @cached_property
    def _base_translator(self) -> IngestionSlingTranslator:
        """Create and cache the custom translator instance."""
        return IngestionSlingTranslator()

    @cached_property
    def sling_resource(self) -> SlingResource:
        """Build the SlingResource with resolved connections.

        This method processes connection configurations from the YAML and:
        1. Resolves environment variables in connection properties
        2. Auto-constructs Databricks http_path from warehouse_id if needed
        3. Creates SlingConnectionResource objects for each connection
        """
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
                        conn_dict[key] = os.environ.get(value[4:], "")
                    elif value.startswith("${") and value.endswith("}"):
                        conn_dict[key] = os.environ.get(value[2:-1], "")

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
        """Execute a Sling replication for the given specification."""
        # Use the processed config file if it exists, otherwise use original
        original_config_path = Path(__file__).parent / replication_spec_model.path
        processed_config_path = original_config_path.parent / f".{original_config_path.stem}_processed.yaml"

        if processed_config_path.exists():
            config_path = processed_config_path
            context.log.info(f"Using processed Sling config: {config_path}")
        else:
            config_path = original_config_path
            context.log.info(f"Using original Sling config: {config_path}")

        yield from self.sling_resource.replicate(
            context=context,
            replication_config=config_path,
            dagster_sling_translator=self._base_translator,
        )
