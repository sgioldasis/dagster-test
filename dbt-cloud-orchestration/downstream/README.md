# Downstream

Downstream processing assets for the Kaizen Wars project.

## Structure

```
downstream/
├── src/
│   └── downstream/
│       ├── __init__.py           # Package exports
│       ├── definitions.py        # Dagster definitions
│       └── defs/
│           ├── __init__.py       # Asset exports
│           └── fact_virtual_count.py  # Asset implementations
├── tests/
│   └── test_fact_virtual_count.py     # Unit tests
├── pyproject.toml
├── workspace.yaml
└── README.md
```

## Development

### Setup

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration/downstream
uv sync
```

### Run Tests

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration/downstream
uv run pytest -v
```

### Run Dagster Development Server

```bash
cd /home/savas/dagster-test/dbt-cloud-orchestration/downstream
dg dev --workspace workspace.yaml
```

Open http://localhost:3000 to see the downstream code location.

## Assets

| Asset | Description | Dependencies |
|-------|-------------|--------------|
| `fact_virtual_count` | Counts records in `stg_kaizen_wars__fact_virtual` and writes to JSON | `dbt_optimove/stg_kaizen_wars__fact_virtual` |

## Environment Variables

Copy `.env` from the monorepo root or set these variables:

```bash
export DBT_CLOUD_TOKEN=your_token_here
export DBT_CLOUD_ACCOUNT_ID=your_account_id
export DBT_CLOUD_ACCESS_URL=https://cloud.getdbt.com
export OUTPUT_DIR=./output
```

## Configuration

The asset reads configuration from environment variables:

- `DBT_CLOUD_TOKEN` - dbt Cloud API token
- `DBT_CLOUD_ACCOUNT_ID` - dbt Cloud account ID
- `DBT_CLOUD_ACCESS_URL` - dbt Cloud access URL (e.g., `https://cloud.getdbt.com`)
- `OUTPUT_DIR` - Directory for output files (default: `./output`)
