import logging

from sync import _databricks as databricks
from sync.api import projects
from sync.api import workspace
from sync.models import Platform

logger = logging.getLogger(__name__)

compute_provider_platform_dict = {
    "aws": Platform.AWS_DATABRICKS,
    "azure": Platform.AZURE_DATABRICKS,
}


def apply_sync_gradient_cluster_recommendation(
        run_submit_task: dict,
        gradient_app_id: str,
        auto_apply: bool,
        cluster_log_url: str,
        workspace_id: str,
) -> dict:
    tasks = run_submit_task.get("tasks")
    if tasks:
        if len(tasks) > 1:
            logger.error(
                "Unable to apply gradient configuration for run submit " "with multiple tasks."
            )
        original_cluster = tasks[0]["new_cluster"]
    else:
        original_cluster = run_submit_task["new_cluster"]

    response = workspace.get_workspace_config(workspace_id)
    workspace_config = response.result

    project_id = create_or_fetch_project(
        gradient_app_id, cluster_log_url, workspace_config.get("compute_provider")
    )

    # if recommendation exists apply the configuration or apply default configuration with project settings
    if auto_apply:
        logger.info(
            f"Generating recommendation - app_id: {gradient_app_id}, project_id: {project_id}, "
            f"auto_apply: {auto_apply}"
        )
        rec = get_gradient_recommendation(project_id)
        if rec:
            logger.info(
                f"Recommendation generated - app_id: {gradient_app_id}, project_id: {project_id}, "
                f"recommendation: {rec}"
            )
            updated_cluster = rec
        else:
            logger.warning(
                f"Unable to generate recommendation. Falling back to original cluster - "
                f"app_id: {gradient_app_id}, project_id: {project_id}, "
                f"auto_apply: {auto_apply}, cluster: {original_cluster}"
            )
            updated_cluster = original_cluster
    else:
        updated_cluster = original_cluster

    resp = databricks.get_project_cluster(updated_cluster, project_id)
    if resp.result:
        configured_cluster = resp.result
        if run_submit_task.get("tasks"):
            run_submit_task["tasks"][0]["new_cluster"] = configured_cluster
        else:
            run_submit_task["new_cluster"] = configured_cluster
        run_submit_task = apply_webhook_notification(workspace_config, run_submit_task)
    else:
        logger.error(
            "Unable to apply gradient configuration to databricks run submit call. "
            "Submitting original run submit call."
        )

    return run_submit_task


def create_or_fetch_project(app_id: str, cluster_log_url: str, compute_provider: str) -> str:
    resp = projects.get_project_by_app_id(app_id)
    if resp.result is None:
        logger.info(
            f"Project with app_id does not exist. Creating project - app_id:{app_id}, "
            f"cluster_log_url:{cluster_log_url}, compute_provider:{compute_provider}"
        )
        resp = projects.create_project(
            name=app_id,
            app_id=app_id,
            cluster_log_url=cluster_log_url,
            product_code=compute_provider_platform_dict.get(compute_provider) or Platform.AWS_DATABRICKS,
        )
    else:
        logger.info(f"Found project with app_id - app_id:{app_id}")

    return resp.result["id"]


def get_gradient_recommendation(project_id: str) -> dict:
    """
    Generates/retrieves the recommendation and returns the cluster configuration

    Args: None

    Returns: cluster config
    """
    response = projects.create_project_recommendation(project_id)
    recommendation_id = response.result

    if recommendation_id is None:
        return None

    response = projects.wait_for_recommendation(project_id, recommendation_id)

    return response.result["recommendation"]["configuration"]


def apply_webhook_notification(workspace_config: dict, task: dict) -> dict:
    webhook_id = workspace_config["webhook_id"]
    if workspace_config.get("collection_type") == "hosted":
        task = append_webhook(task, webhook_id, "on_start")
    else:
        task = append_webhook(task, webhook_id, "on_success")
    return task


def append_webhook(task: dict, webhook_id: str, event: str) -> dict:
    webhook_request = {"id": webhook_id}
    task["webhook_notifications"] = task.get("webhook_notifications") or {}
    task["webhook_notifications"][event] = task["webhook_notifications"].get(event) or []
    task["webhook_notifications"][event].append(webhook_request)

    return task
