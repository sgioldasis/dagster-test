"""Ingestion package for Dagster + Sling/DLT integration.

This package provides data ingestion capabilities using Dagster components
with support for both Sling and dlt (data load tool) for data movement
between CSV files, PostgreSQL, and Databricks.

Main Components:
    IngestionSlingComponent: Custom component for Sling-based replications
        Located in: ingestion/components/ingestion_sling/
    IngestionDltComponent: Component for dlt-based data loading
        Located in: ingestion/components/ingestion_dlt/

Package Structure:
    ingestion/
    ├── __init__.py                 # This file - exports main components
    ├── definitions.py              # Dagster definitions and assets
    ├── components/                 # Component-based assets
    │   ├── ingestion_sling/        # Sling replication component
    │   │   ├── component.py        # Component implementation
    │   │   ├── component.yaml      # Component configuration
    │   │   ├── replication_csv.yaml    # CSV → PostgreSQL replication
    │   │   └── replication_db.yaml     # PostgreSQL → Databricks replication
    │   └── ingestion_dlt/          # DLT ingestion component
    │       ├── dlt_pipeline.py     # DLT resources and sources
    │       ├── loads.py            # Pipeline configurations
    │       └── component.yaml      # Component configuration
    └── defs/                       # Programmatic definitions
        ├── resources.py            # Resource configurations
        └── utils.py                # Utility functions

Usage:
    This package is loaded by Dagster via workspace.yaml:
    ```yaml
    load_from:
      - python_module:
          module_name: "ingestion.definitions"
          attribute: "ingestion_defs"
    ```
"""

from ingestion.components.ingestion_sling.component import IngestionSlingComponent

__all__ = ["IngestionSlingComponent"]
