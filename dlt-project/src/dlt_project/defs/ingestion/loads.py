# src/dlt_project/defs/ingestion/loads.py
import dlt
from dlt.destinations import duckdb  # Import destination class directly

from . import dlt_pipeline

# Create pipeline using destination class directly
pipeline = dlt.pipeline(
    pipeline_name="target_pipeline",
    destination=duckdb(credentials="/tmp/dlt_jaffle_platform.duckdb", normalize=True),
    dataset_name="main",
    # full_refresh=True,
)

# Import target_data from dlt_pipeline.py (instantiated source)
target_data = dlt_pipeline.target_data()
target_data = dlt_pipeline.target_data()
