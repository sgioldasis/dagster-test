# src/dbx_project/definitions.py

from pathlib import Path
from dagster import load_from_defs_folder, Definitions, multiprocess_executor
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

defs = Definitions.merge(
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