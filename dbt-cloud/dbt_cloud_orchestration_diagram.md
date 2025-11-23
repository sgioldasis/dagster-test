# Dagster + dbt Cloud Orchestration Flow

## Architecture Overview

```mermaid
graph TD
    A[Dagster Definitions] --> B[DbtCloudCredentials]
    A --> C[DbtCloudWorkspace]
    
    B --> D[dbt Cloud API Auth]
    C --> E[Project Environment Config]
    
    subgraph Assets
        F["my_dbt_cloud_assets\n@dbt_cloud_assets"]
    end
    
    subgraph Automation
        G["map_asset_specs\nAutomationCondition.eager()"]
        H["AutomationConditionSensorDefinition\npolls every 1s"]
    end
    
    subgraph Sensors
        I["dbt_cloud_polling_sensor\nbuild_dbt_cloud_polling_sensor"]
    end
    
    F --> J[dbt Cloud CLI<br/>dbt build]
    J --> K[dbt Cloud Jobs Run]
    
    H -.-> F
    I -.-> K
    K -.-> L[Asset Materialization Status]
    L -.-> F
    
    %% Default Mermaid styles for readability (no custom fills/strokes)
```

## Execution Flow

```mermaid
sequenceDiagram
    participant D as Dagster
    participant A as dbt Cloud Assets
    participant W as dbt Cloud Workspace
    participant S as Sensors
    
    D->>A: Trigger Materialize
    A->>W: dbt build command
    W->>W: Execute dbt jobs
    Note over W: "dbt models run\nwith dependencies"
    
    S->>W: Poll run status
    W->>S: "Status updates\n(Success/Failed/Error)"
    S->>D: Update asset state
    D->>D: Freshness lineage updated
```

## Key Components Mapping

| Component | Purpose | Line |
|-----------|---------|------|
| DbtCloudCredentials | API authentication | 10 |
| DbtCloudWorkspace | Project/env config | 17 |
| @dbt_cloud_assets | Asset discovery/execution | 25 |
| AutomationCondition.eager() | Auto-materialize | 36 |
| AutomationConditionSensorDefinition | Trigger monitoring | 40 |
| dbt_cloud_polling_sensor | Run status sync | 49 |