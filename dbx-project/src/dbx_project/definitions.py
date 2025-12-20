# src/dbx_project/definitions.py

from pathlib import Path
from dagster import load_from_defs_folder, Definitions, multiprocess_executor, LegacyFreshnessPolicy, AssetKey, AssetSpec
from dagster_databricks import PipesDatabricksClient
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
import os
from .custom_pipes import CustomPipesDatabricksClient

# You still need this to make the env var available to the component
load_dotenv()

# Configure Pipes Client
workspace_client = WorkspaceClient(
    host=os.environ.get("DATABRICKS_HOST"),
    token=os.environ.get("DATABRICKS_TOKEN"),
)

client_kwargs = {}
volume_path = os.environ.get("DATABRICKS_VOLUME_PATH")

if volume_path:
    # Use Unity Catalog Volumes (required for permission restrictions or serverless)
    from dagster_databricks.pipes import (
        PipesUnityCatalogVolumesContextInjector,
        PipesUnityCatalogVolumesMessageReader,
    )
    client_kwargs["context_injector"] = PipesUnityCatalogVolumesContextInjector(
        client=workspace_client, volume_path=volume_path
    )
    client_kwargs["message_reader"] = PipesUnityCatalogVolumesMessageReader(
        client=workspace_client, volume_path=volume_path
    )

def get_definitions() -> Definitions:
    merged_defs = Definitions.merge(
        load_from_defs_folder(
            path_within_project=Path(__file__).parent / 'defs'
        ),
        Definitions(
            executor=multiprocess_executor.configured({"max_concurrent": 4}),
            resources={
                "pipes_databricks": CustomPipesDatabricksClient(
                    client=workspace_client,
                    **client_kwargs
                )
            }
        )
    )

    # Enrich the customers asset with a LegacyFreshnessPolicy to show the "Expected: 2m" label in UI
    def enrich_asset(asset_def):
        customers_key = AssetKey(["target", "main", "customers"])
        # Check if this asset definition contains our target key
        if hasattr(asset_def, "keys") and customers_key in asset_def.keys:
            def transform_spec(spec):
                if spec.key == customers_key:
                    return AssetSpec(
                        key=spec.key,
                        deps=spec.deps,
                        description=spec.description,
                        metadata=spec.metadata,
                        group_name=spec.group_name,
                        skippable=spec.skippable,
                        code_version=spec.code_version,
                        automation_condition=spec.automation_condition,
                        owners=spec.owners,
                        tags=spec.tags,
                        partitions_def=spec.partitions_def,
                        legacy_freshness_policy=LegacyFreshnessPolicy(maximum_lag_minutes=2)
                    )
                return spec
            
            return asset_def.map_asset_specs(transform_spec)
        return asset_def

    enriched_assets = [enrich_asset(a) for a in (merged_defs.assets or [])]

    return Definitions(
        assets=enriched_assets,
        asset_checks=merged_defs.asset_checks,
        resources=merged_defs.resources,
        executor=merged_defs.executor,
        sensors=merged_defs.sensors,
        schedules=merged_defs.schedules,
        jobs=merged_defs.jobs,
    )

defs = get_definitions()