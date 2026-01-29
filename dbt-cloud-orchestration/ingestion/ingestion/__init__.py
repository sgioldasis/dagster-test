"""Ingestion package for Dagster + Sling integration.

This package provides data ingestion capabilities using Dagster components
and Sling for data movement between CSV files, PostgreSQL, and Databricks.

Main Components:
    IngestionSlingComponent: Custom component for Sling-based replications
        Located in: ingestion/components/ingestion_sling/

Package Structure:
    ingestion/
    ├── __init__.py                 # This file - exports main components
    ├── definitions.py              # Dagster definitions and assets
    ├── components/                 # Component-based assets
    │   └── ingestion_sling/        # Sling replication component
    │       ├── component.py        # Component implementation
    │       ├── component.yaml      # Component configuration
    │       ├── replication_csv.yaml    # CSV → PostgreSQL replication
    │       └── replication_db.yaml     # PostgreSQL → Databricks replication
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
