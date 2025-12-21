import os
from dagster import asset, AssetExecutionContext, Definitions, AssetKey, LegacyFreshnessPolicy
from dagster_databricks import PipesDatabricksClient
from databricks.sdk.service import jobs

@asset(
    deps=[AssetKey(["target", "main", "customers"])],
    group_name="post_processing",
    compute_kind="databricks",
    legacy_freshness_policy=LegacyFreshnessPolicy(maximum_lag_minutes=2)
)
def databricks_notebook_job(context: AssetExecutionContext, pipes_databricks: PipesDatabricksClient):
    notebook_path = os.environ.get("DATABRICKS_NOTEBOOK_PATH", "/Users/your.email@databricks.com/dagster_test_notebook")
    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    use_serverless = os.environ.get("DATABRICKS_USE_SERVERLESS", "false").lower() == "true"

    task_params = {
        "task_key": "dagster_notebook_task",
        "notebook_task": jobs.NotebookTask(
            notebook_path=notebook_path
        )
    }

    if use_serverless:
        # For serverless, do not specify cluster configuration
        pass
    elif cluster_id:
        task_params["existing_cluster_id"] = cluster_id
    else:
        # Fallback to a minimal new cluster if no ID provided
        task_params["new_cluster"] = jobs.ClusterSpec(
            spark_version="12.2.x-scala2.12",
            node_type_id="i3.xlarge", 
            num_workers=0
        )

    return pipes_databricks.run(
        context=context,
        task=jobs.SubmitTask(**task_params),
        extras={
            "job_parameters": {
                "param1": "value1"
            }
        }
    ).get_materialize_result()

if os.environ.get("DAGSTER_ENV") == "LOCAL":
    defs = Definitions()
else:
    defs = Definitions(
        assets=[databricks_notebook_job]
    )
