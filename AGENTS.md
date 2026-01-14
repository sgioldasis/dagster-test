# AGENTS.md

This file contains guidelines for agentic coding assistants working in this Dagster monorepo.

## Build, Lint, and Test Commands

### Dependency Management
```bash
cd <project-folder> && uv sync
task sync -- <project-folder>
```

### Running Tests
```bash
task test -- <project-folder>
cd <project-folder> && uv run pytest -v
# Single test: cd <project-folder> && uv run pytest tests/test_file.py::TestClass::test_method -v
```

### Running Dagster Development Server
```bash
task dg-dev -- <project-folder>
cd <project-folder> && dg dev
```

### Available Projects
- `dlt-project` - Dagster + dlt integration (Python 3.9-3.13)
- `dbx-project` - Dagster + Databricks integration (Python 3.10-3.13)
- `sling-project` - Dagster + Sling integration
- `dbt-cloud-orchestration` - Dagster + dbt Cloud orchestration (Python 3.10-3.14)

## Code Style Guidelines

### Imports
Organize: stdlib → third-party → local. Group by type.

### Type Hints
Always include type hints for function parameters and return values.

### Naming Conventions
- Functions/variables: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private methods: `_leading_underscore`

### File Organization
- `src/<project_name>/` - Main source code
- `src/<project_name>/defs/` - Dagster definitions and assets
- `tests/` - Test files (use `test_` prefix)
- `data/` - Data files

### Dagster Patterns

```python
from dagster import asset, AssetExecutionContext, AssetKey, Definitions, load_from_defs_folder, MonthlyPartitionsDefinition
from pathlib import Path

@asset(key=AssetKey(["target", "main", "table_name"]), group_name="ingestion")
def my_asset(context: AssetExecutionContext):
    pass

@definitions
def defs():
    base_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions(assets=base_defs.assets, resources=base_defs.resources, schedules=base_defs.sensors)

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01", end_offset=1)

@asset(partitions_def=monthly_partitions)
def partitioned_asset(context: AssetExecutionContext):
    partition_key = context.partition_key
```

### Error Handling
```python
try:
    result = risky_operation()
except FileNotFoundError as e:
    context.log.warning(f"File not found: {e}")
    return
except Exception as e:
    context.log.error(f"Unexpected error: {e}")
    raise
```

### Environment Variables
```python
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.environ.get("LOCAL_DUCKDB_PATH", "/tmp/default.duckdb")
```

### Database Connections
Always use context managers for database connections:
```python
import duckdb

db_path = _duckdb_path()
con = duckdb.connect(database=db_path, read_only=False)
try:
    con.execute("INSERT INTO table VALUES (...)")
finally:
    con.close()
```

### Path Operations
Prefer `pathlib.Path` over `os.path`:
```python
from pathlib import Path

csv_path = Path("data") / "file.csv"
if csv_path.exists():
    df = pd.read_csv(csv_path)
```

### Docstrings
Use Google-style or simple one-line docstrings:
```python
def _write_partition_to_table(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    """Delete partition rows and insert new data."""
```

### Testing Patterns
```python
import pytest
from dagster import build_asset_context

class TestMyAsset:
    def test_asset_success(self):
        context = build_asset_context()
        result = my_asset(context)
        assert result is not None
```

### Automation Conditions
```python
from dagster import AutomationCondition

asset_def = asset_def.map_asset_specs(
    lambda spec: spec._replace(automation_condition=AutomationCondition.eager())
)
```

### DLT Patterns
```python
import dlt

@dlt.resource(name="resource_name", write_disposition="replace")
def my_resource():
    df = pd.read_csv("data.csv")
    yield from df.to_dict(orient="records")

@dlt.source
def my_source():
    yield my_resource
```

### Scheduling and Logging
```python
from dagster import ScheduleDefinition

my_schedule = ScheduleDefinition(
    name="my_schedule",
    target=AssetSelection.keys("my_asset"),
    cron_schedule="*/5 * * * *",
)

context.log.info("Processing data")
context.log.warning("Warning message")
context.log.error("Error message")
```
