
from dagster_databricks import PipesDatabricksClient
from dagster._core.execution.context.op_execution_context import OpExecutionContext
from dagster._core.execution.context.asset_execution_context import AssetExecutionContext
from dagster._core.pipes.context import PipesSession
from typing import Union, Any, Dict

class CustomPipesDatabricksClient(PipesDatabricksClient):
    def _enrich_submit_task_dict(
        self,
        context: Union[OpExecutionContext, AssetExecutionContext],
        session: PipesSession,
        submit_task_dict: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Check if we are in a "serverless" or "no cluster config" scenario
        # logic inspired by original, but removing strict existing_cluster_id check for notebook injection
        
        has_existing_cluster = "existing_cluster_id" in submit_task_dict
        has_new_cluster = "new_cluster" in submit_task_dict
        
        if has_existing_cluster or (not has_new_cluster and "notebook_task" in submit_task_dict):
            # If existing cluster OR (no cluster config AND notebook task -> likely serverless)
            # We treat it similarly to existing cluster path for notebooks: inject into base_parameters
            
            if "notebook_task" in submit_task_dict:
                existing_params = submit_task_dict["notebook_task"].get("base_parameters", {})
                # merge the existing parameters with the CLI arguments
                existing_params = {**existing_params, **session.get_bootstrap_env_vars()}
                submit_task_dict["notebook_task"]["base_parameters"] = existing_params
            
            # If we had existing_cluster_id, we might also check for other task types like original code
            # But for this specific fix, we focus on notebook_task
            
            # If we are here because of has_existing_cluster=True, we should run original logic for other task types?
            # It's safer to copy the logic or call super if strict conditions met, but super is broken for our case.
            # So we reimplement the safe part.
            
            cli_args = session.get_bootstrap_cli_arguments()
            for task_type in self.get_task_fields_which_support_cli_parameters():
                if task_type in submit_task_dict:
                     existing_params = submit_task_dict[task_type].get("parameters", [])
                     for key, value in cli_args.items():
                        existing_params.extend([key, value])
                     submit_task_dict[task_type]["parameters"] = existing_params
                     break
            
        else:
            # Fallback to new_cluster logic (injecting into spark_env_vars)
            # This will still crash if new_cluster is missing but we didn't match the if above.
            # But if we are serverless notebook, we matched above.
            
            if has_new_cluster:
                pipes_env_vars = session.get_bootstrap_env_vars()
                submit_task_dict["new_cluster"]["spark_env_vars"] = {
                    **submit_task_dict["new_cluster"].get("spark_env_vars", {}),
                    **(self.env or {}),
                    **pipes_env_vars,
                }
        
        submit_task_dict["tags"] = {
            **submit_task_dict.get("tags", {}),
            **session.default_remote_invocation_info,
        }

        return submit_task_dict
