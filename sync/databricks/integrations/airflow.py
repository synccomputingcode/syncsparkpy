import logging
from sync.databricks.integrations._run_submit_runner import apply_sync_gradient_cluster_recommendation

logger = logging.getLogger(__name__)

def airflow_gradient_pre_execute_hook(context):
    try:
        logger.info("Running airflow gradient pre-execute hook!")
        logger.debug(f"Airflow operator context - context:{context}")

        gradient_app_id = context["params"]["gradient_app_id"]
        auto_apply = context["params"]["gradient_auto_apply"]
        cluster_log_url = context["params"]["cluster_log_url"]
        workspace_id = context["params"]["databricks_workspace_id"]
        run_submit_task = context["task"].json.copy()  # copy the run submit json from the task context

        updated_task_configuration = apply_sync_gradient_cluster_recommendation(
            run_submit_task=run_submit_task,
            gradient_app_id=gradient_app_id,
            auto_apply=auto_apply,
            cluster_log_url=cluster_log_url,
            workspace_id=workspace_id
        )

        context["task"].json = updated_task_configuration
    except Exception as e:
        logger.exception(e)
        logger.error("Unable to apply gradient configuration to Databricks run submit tasks")
