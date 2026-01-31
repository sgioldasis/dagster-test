"""DLT Ingestion Component.

This package provides dlt-based data ingestion capabilities as an alternative
to the Sling-based ingestion. It defines:

- dlt_csv_fact_virtual: Asset for loading CSV data into PostgreSQL
- dlt_databricks_fact_virtual: Asset for loading PostgreSQL data into Databricks

Usage:
    The component is automatically loaded by Dagster's component system
    through the component.yaml configuration file.
"""

from . import dlt_pipeline, loads
from .component import IngestionDltComponent, IngestionDltTranslator

__all__ = ["dlt_pipeline", "loads", "IngestionDltComponent", "IngestionDltTranslator"]
