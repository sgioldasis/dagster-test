"""DBT Cloud code location using component-based configuration.

This module defines the Dagster code location for dbt Cloud orchestration.
Configuration is loaded from component.yaml using the Dagster Components framework,
which handles environment variable resolution automatically.
"""

from pathlib import Path

import dagster as dg
from dagster.components import build_component_defs
from dotenv import load_dotenv

# Load environment variables from .env file
# This ensures {{ env('DBT_CLOUD_TOKEN') }} in defs.yaml can be resolved
load_dotenv()


defs = build_component_defs(
    components_root=Path(__file__).parent / "components",
)


def dbt_defs() -> dg.Definitions:
    """Wrapper function for tools that expect a function attribute."""
    return defs
