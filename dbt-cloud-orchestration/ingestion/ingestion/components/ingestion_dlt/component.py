"""Custom DLT Component for data ingestion.

This module provides a custom Dagster component that extends DltLoadCollectionComponent
to add project-specific behavior for data loading using dlt.

The component provides custom asset naming to match the project's conventions:
- dlt_csv_fact_virtual for CSV → PostgreSQL ingestion  
- dlt_databricks_fact_virtual for PostgreSQL → Databricks ingestion
"""

from functools import cached_property
from typing import Any, Mapping

import dagster as dg
from dagster import AssetKey, AssetSpec
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, DltLoadCollectionComponent
from dagster_dlt.translator import DltResourceTranslatorData


class IngestionDltTranslator(DagsterDltTranslator):
    """Custom translator for dlt resources to Dagster asset specs.

    This translator customizes asset naming to match the project's conventions:
    - dlt_csv_fact_virtual for CSV → PostgreSQL ingestion
    - dlt_databricks_fact_virtual for PostgreSQL → Databricks ingestion

    It also hides source assets that would expose local file paths.
    """

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Convert dlt resource data to a Dagster AssetSpec.

        Args:
            data: DltResourceTranslatorData containing resource info.

        Returns:
            AssetSpec with custom key and metadata.
        """
        # Get the base spec from the parent translator
        spec = super().get_asset_spec(data)

        # Get resource name
        resource_name = data.resource.name

        # Custom naming logic based on resource name and pipeline
        # The pipeline name tells us which ingestion path this is
        pipeline_name = data.pipeline.pipeline_name

        if pipeline_name == "csv_to_postgres" and resource_name == "dlt_fact_virtual":
            # CSV → PostgreSQL asset
            return spec.replace_attributes(
                key=AssetKey(["dlt_csv_fact_virtual"]),
                group_name="ingestion",
                description="Load CSV fact_virtual data into PostgreSQL using dlt",
            )
        elif pipeline_name == "postgres_to_databricks" and resource_name == "dlt_fact_virtual":
            # PostgreSQL → Databricks asset - depends on dlt_csv_fact_virtual
            return spec.replace_attributes(
                key=AssetKey(["dlt_databricks_fact_virtual"]),
                group_name="ingestion",
                description="Load PostgreSQL dlt_fact_virtual data into Databricks test.main.dlt_fact_virtual using dlt",
                deps=[AssetKey(["dlt_csv_fact_virtual"])],
            )

        # For source assets (not the main target assets), hide them by using a sanitized key
        # This prevents exposing local file paths in the asset key
        return spec.replace_attributes(
            key=AssetKey(["dlt_source", resource_name]),
            group_name="ingestion",
        )


class IngestionDltComponent(DltLoadCollectionComponent):
    """Custom DLT component for the ingestion project.

    This component extends DltLoadCollectionComponent to provide:
    1. Custom translator for asset spec generation
    2. Assets named dlt_csv_fact_virtual and dlt_databricks_fact_virtual

    Configuration (in component.yaml):
        type: ingestion.components.ingestion_dlt.IngestionDltComponent
        attributes:
          loads:
            - source: .loads.csv_source
              pipeline: .loads.postgres_pipeline
            - source: .loads.postgres_fact_virtual_source
              pipeline: .loads.databricks_pipeline

    The component creates Dagster assets for each load defined in the YAML.
    """

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build definitions with custom translator.

        Args:
            context: Component load context.

        Returns:
            Definitions with dlt assets.
        """
        from dagster_dlt.asset_decorator import dlt_assets
        from dagster_dlt.components.dlt_load_collection.component import DltLoadSpecModel

        output = []
        for load in self.loads:
            # Create a custom translator for this load
            translator = IngestionDltTranslator()

            # Sanitize the dataset name for use in the op name (dots not allowed)
            dataset_name_sanitized = load.pipeline.dataset_name.replace(".", "_")

            @dlt_assets(
                dlt_source=load.source,
                dlt_pipeline=load.pipeline,
                name=f"dlt_assets_{load.source.name}_{dataset_name_sanitized}",
                dagster_dlt_translator=translator,
            )
            def dlt_assets_def(context: dg.AssetExecutionContext):
                yield from self.execute(context, self.dlt_pipeline_resource)

            output.append(dlt_assets_def)

        return dg.Definitions(assets=output)

    def execute(
        self,
        context: dg.AssetExecutionContext,
        dlt_pipeline_resource: "DagsterDltResource",
    ):
        """Execute the dlt pipeline with write_disposition=replace.

        This ensures full-refresh behavior for all DLT loads.

        Args:
            context: The asset execution context.
            dlt_pipeline_resource: The DLT pipeline resource.

        Yields:
            Events from the dlt pipeline execution.
        """
        yield from dlt_pipeline_resource.run(
            context=context,
            write_disposition="replace",
        )
