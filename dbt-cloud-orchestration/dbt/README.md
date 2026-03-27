# dbt Cloud Orchestration (Component-Based)

This project uses **Dagster Components** to orchestrate dbt Cloud jobs. It provides a declarative, YAML-first approach to configuring dbt Cloud integration, making it easier to manage and understand the pipeline configuration.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Component-Based Architecture                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐      ┌──────────────────┐                  │
│  │  component.yaml │─────▶│  definitions.py  │                  │
│  │  (Declarative)  │      │  (Orchestrator)  │                  │
│  └─────────────────┘      └────────┬─────────┘                  │
│         │                          │                            │
│         │                          ▼                            │
│         │               ┌──────────────────┐                    │
│         │               │ DbtCloudWorkspace│                    │
│         │               └────────┬─────────┘                    │
│         │                        │                              │
│         │                        ▼                              │
│         │               ┌──────────────────┐                    │
│         └──────────────▶│ dbt_cloud_assets │                    │
│                         │ (760+ models)    │                    │
│                         └──────────────────┘                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## File Structure

```
dbt-cloud-orchestration/dbt/
├── pyproject.toml                          # Project configuration
├── workspace.yaml                          # Dagster workspace config
├── README.md                               # This file
├── src/dbt_orchestration/
│   ├── definitions.py                      # Main entry point
│   └── components/
│       └── dbt_cloud/
│           ├── component.yaml              # Declarative configuration
│           └── component.py                # Custom component (reference)
│   └── defs/
│       ├── assets.py                       # Legacy asset definitions
│       ├── resources.py                    # Resource definitions
│       └── constants.py                    # Constants and enums
```

## Core Files Explained

### 1. `component.yaml` - The Heart of Configuration

**Location:** [`src/dbt_orchestration/components/dbt_cloud/component.yaml`](src/dbt_orchestration/components/dbt_cloud/component.yaml)

This file contains all declarative configuration for your dbt Cloud integration. Instead of writing Python code, you configure everything in YAML:

```yaml
type: dbt_orchestration.DbtCloudOrchestrationComponent
attributes:
  workspace:
    account_id: 413                          # Your dbt Cloud account ID
    project_id: 10157                        # Your dbt Cloud project ID
    environment_id: 26829                    # Your dbt Cloud environment ID
    access_url: "https://tw590.eu1.dbt.com"  # Your dbt Cloud access URL
    token: "{{ env('DBT_CLOUD_TOKEN') }}"    # Token from environment variable
  timeout_seconds: 600                       # Job timeout
  target_key: "stg_kaizen_wars__fact_virtual"  # Key asset for downstream deps
  freshness_deadline_cron: "0 6 * * *"       # Freshness check schedule
  freshness_lower_bound_minutes: 1440        # 24 hour freshness window
```

**Key Features:**
- **Environment Variable Substitution:** Use `{{ env('VAR_NAME') }}` to inject secrets from your `.env` file
- **No Python Code Required:** All configuration is declarative
- **Version Control Friendly:** Easy to diff and review changes

### 2. `definitions.py` - The Orchestrator

**Location:** [`src/dbt_orchestration/definitions.py`](src/dbt_orchestration/definitions.py)

This file reads `component.yaml`, processes environment variables, and builds the Dagster definitions:

```python
def _load_component_config() -> dict:
    """Load and parse component.yaml with env var substitution."""
    # 1. Reads the YAML file
    # 2. Substitutes {{ env('VAR_NAME') }} with actual values
    # 3. Returns parsed configuration

def _build_definitions() -> dg.Definitions:
    """Build Dagster definitions from component configuration."""
    # 1. Creates DbtCloudWorkspace from YAML config
    # 2. Defines @dbt_cloud_assets decorated function
    # 3. Creates freshness check sensor
    # 4. Returns complete Definitions object
```

**What it does:**
1. **Environment Variable Substitution:** Replaces `{{ env('DBT_CLOUD_TOKEN') }}` with the actual token from your `.env` file
2. **Workspace Creation:** Creates a `DbtCloudWorkspace` with your credentials
3. **Asset Generation:** Discovers all dbt models and creates Dagster assets
4. **Freshness Checks:** Monitors data freshness based on your cron schedule
5. **Asset Key Translation:** Uses `PackageAwareDbtTranslator` to prefix asset keys with dbt package names (e.g., `dbt_accounts/dim_customer`)

### 3. `component.py` - Custom Component Reference

**Location:** [`src/dbt_orchestration/components/dbt_cloud/component.py`](src/dbt_orchestration/components/dbt_cloud/component.py)

This file defines a custom `DbtCloudOrchestrationComponent` class. While the current implementation loads configuration directly in `definitions.py`, this file serves as:

- **Reference Implementation:** Shows how to create a true custom component
- **Future Extension Point:** Can be activated to use full component lifecycle
- **PackageAwareDbtTranslator:** Custom translator that includes dbt package names in asset keys

**Key Classes:**
- `PackageAwareDbtTranslator`: Ensures asset keys are unique by including package name (e.g., `dbt_accounts/dim_customer` instead of just `dim_customer`)
- `DbtCloudOrchestrationComponent`: Full component implementation with `build_defs()` method

### 4. `pyproject.toml` - Project Configuration

**Location:** [`pyproject.toml`](pyproject.toml)

Contains project metadata and Dagster-specific configuration:

```toml
[tool.dg.project]
root_module = "dbt_orchestration"
registry_modules = [
    "dbt_orchestration.components.*",  # Enable component discovery
]
```

## How to Run Your dbt Cloud Job

### Step 1: Set Up Environment Variables

Create a `.env` file in the monorepo root (`dbt-cloud-orchestration/.env`):

```bash
# Required: Your dbt Cloud API token
DBT_CLOUD_TOKEN=dbtc_your_token_here

# Optional: These are now configured in component.yaml, but can be overridden
DBT_CLOUD_ACCOUNT_ID=413
DBT_CLOUD_PROJECT_ID=10157
DBT_CLOUD_ENVIRONMENT_ID=26829
DBT_CLOUD_ACCESS_URL=https://tw590.eu1.dbt.com
```

**Getting your token:**
1. Log in to dbt Cloud
2. Go to Account Settings → API Tokens
3. Create a new token with appropriate permissions

### Step 2: Configure component.yaml

Edit [`src/dbt_orchestration/components/dbt_cloud/component.yaml`](src/dbt_orchestration/components/dbt_cloud/component.yaml):

```yaml
type: dbt_orchestration.DbtCloudOrchestrationComponent
attributes:
  workspace:
    account_id: 413                          # Update with your account ID
    project_id: 10157                        # Update with your project ID
    environment_id: 26829                    # Update with your environment ID
    access_url: "https://tw590.eu1.dbt.com"  # Update with your access URL
    token: "{{ env('DBT_CLOUD_TOKEN') }}"    # This reads from .env
  timeout_seconds: 600                       # Adjust as needed
  target_key: "stg_kaizen_wars__fact_virtual"  # Your key downstream asset
  freshness_deadline_cron: "0 6 * * *"       # Daily at 6 AM
  freshness_lower_bound_minutes: 1440        # 24 hours
```

### Step 3: Sync Dependencies

```bash
cd dbt-cloud-orchestration/dbt
uv sync
```

### Step 4: Verify Configuration

```bash
# List all definitions to verify everything loads correctly
uv run dg list defs
```

You should see:
- 760+ Assets (dbt models with package prefixes)
- 190+ Asset Checks (dbt tests)
- 2 Sensors (automation + run status)
- 1 Resource (dbt_cloud workspace)

### Step 5: Run the Dagster Dev Server

```bash
# Option 1: Run with workspace.yaml
dg dev --workspace workspace.yaml

# Option 2: Run directly
PYTHONPATH=/path/to/dbt-cloud-orchestration:$PYTHONPATH dg dev
```

Open http://localhost:3000 to see the Dagster UI.

### Step 6: Materialize Assets

1. In the Dagster UI, navigate to the **Assets** tab
2. Find your dbt assets (they'll have keys like `dbt_accounts/dim_customer`)
3. Click **Materialize** to trigger a dbt Cloud run
4. Monitor the run in the Dagster UI

## Configuration Reference

### component.yaml Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `workspace.account_id` | int | required | Your dbt Cloud account ID |
| `workspace.project_id` | int | required | Your dbt Cloud project ID |
| `workspace.environment_id` | int | required | Your dbt Cloud environment ID |
| `workspace.access_url` | string | required | Your dbt Cloud access URL |
| `workspace.token` | string | required | API token (use `{{ env('VAR') }}`) |
| `timeout_seconds` | int | 3600 | Job timeout in seconds |
| `target_key` | string | required | Key asset for downstream dependencies |
| `freshness_deadline_cron` | string | "0 6 * * *" | Cron for freshness checks |
| `freshness_lower_bound_minutes` | int | 1440 | Freshness window in minutes |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DBT_CLOUD_TOKEN` | Yes | dbt Cloud API token |
| `DBT_CLOUD_ACCOUNT_ID` | No* | Account ID (or set in YAML) |
| `DBT_CLOUD_PROJECT_ID` | No* | Project ID (or set in YAML) |
| `DBT_CLOUD_ENVIRONMENT_ID` | No* | Environment ID (or set in YAML) |
| `DBT_CLOUD_ACCESS_URL` | No* | Access URL (or set in YAML) |

*These can be hardcoded in `component.yaml` or set via environment variables.

## Troubleshooting

### 401 Unauthorized Error

**Cause:** Invalid or expired dbt Cloud token

**Solution:**
1. Verify token in `.env` file
2. Check token hasn't expired in dbt Cloud UI
3. Ensure token has correct permissions

### Assets Not Loading

**Cause:** Incorrect workspace configuration

**Solution:**
1. Run `uv run dg list defs` to see error details
2. Verify `account_id`, `project_id`, `environment_id` in `component.yaml`
3. Check `access_url` matches your dbt Cloud region

### Environment Variable Not Found

**Cause:** `.env` file not loaded

**Solution:**
1. Ensure `.env` is in `dbt-cloud-orchestration/` (monorepo root)
2. Check variable name matches exactly
3. Try exporting directly: `export DBT_CLOUD_TOKEN=...`

## Comparison: Component vs. Traditional

| Aspect | Component-Based (Current) | Traditional (Legacy) |
|--------|---------------------------|----------------------|
| Configuration | YAML (`component.yaml`) | Python (`defs/assets.py`) |
| Environment Vars | `{{ env('VAR') }}` syntax | `os.environ.get()` |
| Code Complexity | Low (declarative) | High (imperative) |
| Customization | Moderate | High |
| Learning Curve | Low | High |
| Maintenance | Easy | Requires Python knowledge |

## Next Steps

1. **Customize Freshness Checks:** Adjust `freshness_deadline_cron` and `freshness_lower_bound_minutes` for your SLA
2. **Add Downstream Dependencies:** The `target_key` asset can be used to trigger downstream jobs
3. **Extend the Component:** Modify `component.py` to add custom logic
4. **Add Schedules:** Create schedules in `definitions.py` for automated runs

## Resources

- [Dagster dbt Cloud Documentation](https://docs.dagster.io/integrations/dbt-cloud)
- [Dagster Components Guide](https://docs.dagster.io/guides/build/components/)
- [dbt Cloud API Reference](https://docs.getdbt.com/dbt-cloud/api-v2)
