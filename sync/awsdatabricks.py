"""
Utilities for interacting with Databricks
"""
import io
import logging
import zipfile
from time import sleep
from typing import Any, TypeVar
from urllib.parse import urlparse

import boto3 as boto

from sync.api.predictions import create_prediction_with_eventlog_bytes, get_prediction
from sync.api.projects import get_project
from sync.clients.databricks import get_default_client
from sync.config import CONFIG
from sync.models import DatabricksAPIError, DatabricksError, Platform, Response

logger = logging.getLogger(__name__)


def create_prediction(
    plan_type: str, compute_type: str, eventlog: bytes, project_id: str = None
) -> Response[str]:
    """Create a Databricks prediction

    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param eventlog: encoded event log zip
    :type eventlog: bytes
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    return create_prediction_with_eventlog_bytes(
        Platform.AWS_DATABRICKS,
        {"plan_type": plan_type, "compute_type": compute_type},
        "eventlog.zip",
        eventlog,
        project_id,
    )


def create_cluster(config: dict) -> Response[str]:
    """Create Databricks cluster from the provided configuration.

    The configuration should match that described here: https://docs.databricks.com/dev-tools/api/latest/clusters.html#create

    :param config: cluster configuration
    :type config: dict
    :return: cluster ID
    :rtype: Response[str]
    """
    response = get_default_client().create_cluster(config)
    if "error_code" in response:
        return Response(error=DatabricksAPIError(**response))

    return Response(result=response["cluster_id"])


def get_cluster(cluster_id: str) -> Response[dict]:
    """Get Databricks cluster.

    :param cluster_id: cluster ID
    :type cluster_id: str
    :return: cluster object
    :rtype: Response[dict]
    """
    cluster = get_default_client().get_cluster(cluster_id)
    if "error_code" in cluster:
        return Response(error=DatabricksAPIError(**cluster))

    return Response(result=cluster)


def create_prediction_for_run(
    run_id: str, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    """Create a prediction for the specified Databricks run.

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    run = get_default_client().get_run(run_id)
    if "error_code" in run:
        return Response(error=DatabricksAPIError(**run))

    if run["state"].get("result_state") != "SUCCESS":
        return Response(error=DatabricksError(message="Run did not successfully complete"))

    cluster_response = _get_run_cluster(run["tasks"])
    if cluster := cluster_response.result:
        eventlog_response = _get_eventlog(run["tasks"][0], cluster)
        if eventlog := eventlog_response.result:
            return create_prediction(plan_type, compute_type, eventlog, project_id)
        return eventlog_response

    return cluster_response


def record_run(run_id: str, plan_type: str, compute_type: str, project_id: str) -> Response[str]:
    """See :py:func:`~create_prediction_for_run`

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    return create_prediction_for_run(run_id, plan_type, compute_type, project_id)


def get_prediction_job(
    job_id: str, prediction_id: str, preference: str = CONFIG.default_prediction_preference.value
) -> Response[dict]:
    """Apply the prediction to the specified job.

    The basis job can only have tasks that run on the same cluster. That cluster is updated with the
    configuration from the prediction and returned in the result job configuration. Use this function
    to apply a prediction to an existing job or test a prediction with a one-off run.

    :param job_id: basis job ID
    :type job_id: str
    :param prediction_id: prediction ID
    :type prediction_id: str
    :param preference: preferred prediction solution, defaults to local configuration
    :type preference: str, optional
    :return: job object with prediction applied to it
    :rtype: Response[dict]
    """
    prediction_response = get_prediction(prediction_id)
    if prediction := prediction_response.result:
        job = get_default_client().get_job(job_id)
        if "error_code" in job:
            return Response(error=DatabricksAPIError(**job))

        job_settings = job["settings"]
        if tasks := job_settings.get("tasks", []):
            cluster_response = _get_job_cluster(tasks, job_settings.get("job_clusters", []))
            if cluster := cluster_response.result:
                prediction_cluster = _deep_update(
                    cluster, prediction["solutions"][preference]["configuration"]
                )
                cluster_key = tasks[0]["job_cluster_key"]
                job_settings["job_clusters"] = [
                    j
                    for j in job_settings["job_clusters"]
                    if j.get("job_cluster_key") != cluster_key
                ] + [{"job_cluster_key": cluster_key, "new_cluster": prediction_cluster}]
                return Response(result=job)
            return cluster_response
        return Response(error=DatabricksError(message="No task found in job"))
    return prediction_response


def get_project_job(job_id: str, project_id: str, region_name: str = None) -> Response[dict]:
    """Apply project configuration to a job.

    The job can only have tasks that run on the same job cluster. That cluster is updated with tags
    and a log configuration to facilitate project continuity. The result can be tested in a
    one-off run or applied to an existing job to surface run-time (see :py:func:`~run_job_object`) or cost optimizations.

    :param job_id: ID of basis job
    :type job_id: str
    :param project_id: Sync project ID
    :type project_id: str
    :param region_name: region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: project job object
    :rtype: Response[dict]
    """
    job = get_default_client().get_job(job_id)
    if "error_code" in job:
        return Response(error=DatabricksAPIError(**job))

    job_settings = job["settings"]
    if tasks := job_settings.get("tasks", []):
        cluster_response = _get_job_cluster(tasks, job_settings.get("job_clusters", []))
        if cluster := cluster_response.result:
            project_settings_response = get_project_cluster_settings(project_id, region_name)
            if project_cluster_settings := project_settings_response.result:
                project_cluster = _deep_update(cluster, project_cluster_settings)
                cluster_key = tasks[0]["job_cluster_key"]
                job_settings["job_clusters"] = [
                    j
                    for j in job_settings["job_clusters"]
                    if j.get("job_cluster_key") != cluster_key
                ] + [{"job_cluster_key": cluster_key, "new_cluster": project_cluster}]
                return Response(result=job)
            return project_settings_response
        return cluster_response
    return Response(error=DatabricksError(message="No task found in job"))


def get_project_cluster_settings(project_id: str, region_name: str = None) -> Response[dict]:
    """Gets cluster configuration for a project.

    This configuration is intended to be used to update the cluster of a Databricks job so that
    its runs can be included in a Sync project.

    :param project_id: Sync project ID
    :type project_id: str
    :param region_name: region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: project cluster settings - a subset of a Databricks cluster object
    :rtype: Response[dict]
    """
    project_response = get_project(project_id)
    if project := project_response.result:
        result = {
            "custom_tags": {
                "sync:project-id": project_id,
            }
        }
        if s3_url := project.get("s3_url"):
            result.update(
                {
                    "cluster_log_conf": {
                        "s3": {
                            "destination": f"{s3_url}/{project_id}",
                            "enable_encryption": True,
                            "region": region_name or boto.client("s3").meta.region_name,
                            "canned_acl": "bucket-owner-full-control",
                        }
                    }
                }
            )
        return Response(result=result)
    return project_response


def run_job_object(job: dict) -> Response[str]:
    """Create a Databricks one-off run based on the job configuration.

    :param job: Databricks job object
    :type job: dict
    :return: run ID
    :rtype: Response[str]
    """
    tasks = job["settings"]["tasks"]
    cluster_response = _get_job_cluster(tasks, job["settings"].get("job_clusters", []))

    if cluster := cluster_response.result:
        if len(tasks) == 1:
            tasks[0]["new_cluster"] = cluster
            del tasks[0]["job_cluster_key"]
        else:
            # Create an "All-Purpose Compute" cluster
            cluster["cluster_name"] = cluster["cluster_name"] or job["settings"]["name"]
            cluster["autotermination_minutes"] = 10  # 10 minutes is the minimum

            cluster_result = get_default_client().create_cluster(cluster)
            if "error_code" in cluster_result:
                return Response(error=DatabricksAPIError(**cluster_result))

            for task in tasks:
                task["existing_cluster_id"] = cluster_result["cluster_id"]
                if "new_cluster" in task:
                    del task["new_cluster"]
                if "job_cluster_key" in task:
                    del task["job_cluster_key"]

        run_result = get_default_client().create_run(
            {"run_name": job["settings"]["name"], "tasks": tasks}
        )
        if "error_code" in run_result:
            return Response(error=DatabricksAPIError(**run_result))

        return Response(result=run_result["run_id"])
    return cluster_response


def run_prediction(job_id: str, prediction_id: str, preference: str) -> Response[str]:
    """Create a one-off Databricks run based on the prediction applied to the job.

    :param job_id: job ID
    :type job_id: str
    :param prediction_id: prediction ID
    :type prediction_id: str
    :param preference: preferred prediction solution
    :type preference: str
    :return: run ID
    :rtype: Response[str]
    """
    prediction_job_response = get_prediction_job(job_id, prediction_id, preference)
    if prediction_job := prediction_job_response.result:
        return run_job_object(prediction_job)
    return prediction_job_response


def create_run(run: dict) -> Response[str]:
    """Creates a run based off the incoming Databricks run configuration

    :param run: run object
    :type run: dict
    :return: run ID
    :rtype: Response[str]
    """
    run_result = get_default_client().create_run(run)
    if "error_code" in run_result:
        return Response(error=DatabricksAPIError(**run_result))

    return Response(result=run_result["run_id"])


def run_and_record_prediction_job(
    job_id: str,
    prediction_id: str,
    plan_type: str,
    compute_type: str,
    project_id: str = None,
    preference: str = CONFIG.default_prediction_preference.value,
) -> Response[str]:
    """Run a prediction applied to the specified job and record the result.

    This function waits for the run to complete before creating a new prediction based on that run.
    If a project is specified the new prediction is added to it.

    :param job_id: basis job ID
    :type job_id: str
    :param prediction_id: project ID
    :type prediction_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :param preference: preferred prediction solution, defaults to local configuration
    :type preference: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    prediction_job_response = get_prediction_job(job_id, prediction_id, preference)
    if prediction_job := prediction_job_response.result:
        return run_and_record_job_object(prediction_job, plan_type, compute_type, project_id)
    return prediction_job_response


def run_and_record_project_job(
    job_id: str, project_id: str, plan_type: str, compute_type: str, region_name: str = None
) -> Response[str]:
    """Runs the specified job and adds the result to the project.

    This function waits for the run to complete.

    :param job_id: Databricks job ID
    :type job_id: str
    :param project_id: Sync project ID
    :type project_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param region_name: region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    project_job_response = get_project_job(job_id, project_id, region_name)
    if project_job := project_job_response.result:
        return run_and_record_job_object(project_job, plan_type, compute_type, project_id)
    return project_job_response


def run_and_record_job(
    job_id: str, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    """Runs the specified job and creates a prediction based on the result.

    If a project is specified the prediction is added to it.

    :param job_id: Databricks job ID
    :type job_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    # creates a "Jobs Compute" cluster
    run_result = get_default_client().create_job_run({"job_id": job_id})
    if "error_code" in run_result:
        return Response(error=DatabricksAPIError(**run_result))

    run_id = run_result["run_id"]
    return wait_for_and_record_run(run_id, plan_type, compute_type, project_id)


def run_and_record_job_object(
    job: dict, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    """Creates a one-off Databricks run based on the provided job object.

    Job tasks must use the same job cluster, and that cluster must be configured to store the
    event logs in S3.

    :param job: Databricks job object
    :type job: dict
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    run_response = run_job_object(job)
    if run_id := run_response.result:
        wait_response = wait_for_run_and_cluster(run_id)
        if result_state := wait_response.result:
            if result_state == "SUCCESS":
                return record_run(run_id, plan_type, compute_type, project_id)
            return Response(
                error=DatabricksError(message=f"Unsuccessful run result state: {result_state}")
            )
        return wait_response
    return run_response


def create_and_record_run(
    run: dict, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    """Applies the Databricks run configuration and creates a prediction based on the result.

    If a project is specified the resulting prediction is added to it. This function waits for
    run to complete.

    :param run: Databricks run configuration
    :type run: dict
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    run_response = create_run(run)
    if run_id := run_response.result:
        return wait_for_and_record_run(run_id, plan_type, compute_type, project_id)
    return run_response


def wait_for_and_record_run(
    run_id: str, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    """Waits for a run to complete before creating a prediction.

    The run must save 1 event log to S3. If a project is specified the prediction is added
    to that project.

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    wait_response = wait_for_final_run_status(run_id)
    if result_state := wait_response.result:
        if result_state == "SUCCESS":
            return record_run(run_id, plan_type, compute_type, project_id)
        return Response(
            error=DatabricksError(message=f"Unsuccessful run result state: {result_state}")
        )
    return wait_response


def create_and_wait_for_run(run: dict) -> Response[str]:
    """Creates a Databricks run from the incoming configuration and returns the final status.

    This function waits for the run to complete.

    :param run: Databricks run configuration
    :type run: dict
    :return: result state, e.g. "SUCCESS"
    :rtype: Response[str]
    """
    run_response = create_run(run)
    if run_response.error:
        return run_response

    return wait_for_final_run_status(run_response.result)


def wait_for_final_run_status(run_id: str) -> Response[str]:
    """Waits for run returning final status.

    :param run_id: Databricks run ID
    :type run_id: str
    :return: result state, e.g. "SUCCESS"
    :rtype: Response[str]
    """
    run = get_default_client().get_run(run_id)
    while "error_code" not in run:
        result_state = run["state"].get("result_state")  # result_state isn't present while running
        if result_state in {"SUCCESS", "FAILED", "TIMEDOUT", "CANCELED"}:
            return Response(result=result_state)

        sleep(30)
        run = get_default_client().get_run(run_id)

    return Response(error=DatabricksAPIError(**run))


def wait_for_run_and_cluster(run_id: str) -> Response[str]:
    """Waits for final run status and returns it after terminating the cluster.

    :param run_id: Databricks run ID
    :type run_id: str
    :return: result state, e.g. "SUCCESS"
    :rtype: Response[str]
    """
    run = get_default_client().get_run(run_id)
    while "error_code" not in run:
        result_state = run["state"].get("result_state")  # result_state isn't present while running
        if result_state in {"SUCCESS", "FAILED", "TIMEDOUT", "CANCELED"}:
            existing_cluster_ids = list({task.get("existing_cluster_id") for task in run["tasks"]})
            for cluster_id in existing_cluster_ids:
                cluster_response = terminate_cluster(cluster_id)
                if cluster_response.error:
                    return cluster_response
            return Response(result=result_state)

        sleep(30)
        run = get_default_client().get_run(run_id)

    return Response(error=DatabricksAPIError(**run))


def terminate_cluster(cluster_id: str) -> Response[str]:
    """Terminates Databricks cluster and waits to return final state.

    :param cluster_id: Databricks cluster ID
    :type cluster_id: str
    :return: terminal state: "TERMINATED"
    :rtype: Response[str]
    """
    cluster = get_default_client().get_cluster(cluster_id)
    while "error_code" not in cluster:
        state = cluster.get("state")
        match state:
            case "TERMINATED":
                return Response(result=state)
            case "TERMINATING":
                sleep(30)
            case "PENDING" | "RUNNING" | "RESTARTING" | "RESIZING":
                get_default_client().delete_cluster(cluster_id)
            case _:
                return Response(error=DatabricksError(message=f"Unexpected cluster state: {state}"))

        cluster = get_default_client().get_cluster(cluster_id)

    return Response(error=DatabricksAPIError(**cluster))


def _get_job_cluster(tasks: list[dict], job_clusters: list) -> Response[dict]:
    if len(tasks) == 1:
        return _get_task_cluster(tasks[0], job_clusters)

    if [t.get("job_cluster_key") for t in tasks].count(tasks[0].get("job_cluster_key")) == len(
        tasks
    ):
        for cluster in job_clusters:
            if cluster["job_cluster_key"] == tasks[0].get("job_cluster_key"):
                return Response(result=cluster["new_cluster"])
        return Response(error=DatabricksError(message="No cluster found for task"))
    return Response(error=DatabricksError(message="Not all tasks use the same cluster"))


def _get_run_cluster(tasks: list[dict]) -> Response[dict]:
    cluster_ids = {task["cluster_instance"]["cluster_id"] for task in tasks}
    match len(cluster_ids):
        case 1:
            cluster = get_default_client().get_cluster(cluster_ids.pop())
            if "error_code" in cluster:
                return Response(error=DatabricksAPIError(**cluster))
            return Response(result=cluster)
        case 0:
            return Response(error=DatabricksError(message="No cluster found for tasks"))
        case _:
            return Response(error=DatabricksError(message="More than 1 cluster found for tasks"))


def _get_task_cluster(task: dict, clusters: list) -> Response[dict]:
    cluster = task.get("new_cluster")

    if not cluster:
        if cluster_matches := [
            candidate
            for candidate in clusters
            if candidate["job_cluster_key"] == task.get("job_cluster_key")
        ]:
            cluster = cluster_matches[0]["new_cluster"]
        else:
            return Response(error=DatabricksError(message="No cluster found for task"))
    return Response(result=cluster)


def _get_eventlog(task: dict, cluster: dict) -> Response[bytes]:
    log_url = cluster.get("cluster_log_conf", {}).get("s3", {}).get("destination")
    if log_url:
        parsed_log_url = urlparse(log_url)

        s3 = boto.client("s3")
        contents = s3.list_objects_v2(
            Bucket=parsed_log_url.netloc,
            Prefix=f"{parsed_log_url.path.strip('/')}/{task.get('cluster_instance', {}).get('cluster_id')}/eventlog/{task.get('cluster_instance').get('cluster_id')}",
        ).get("Contents")

        if contents:
            eventlog_zip = io.BytesIO()
            eventlog_zip_file = zipfile.ZipFile(eventlog_zip, "a", zipfile.ZIP_DEFLATED)

            for content in contents:
                obj = s3.get_object(Bucket=parsed_log_url.netloc, Key=content["Key"])
                eventlog_zip_file.writestr(content["Key"].split("/")[-1], obj["Body"].read())

            eventlog_zip_file.close()

            return Response(result=eventlog_zip.getvalue())

        return Response(error=DatabricksError(message="No eventlog found"))


KeyType = TypeVar("KeyType")


def _deep_update(
    mapping: dict[KeyType, Any], *updating_mappings: dict[KeyType, Any]
) -> dict[KeyType, Any]:
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if k in updated_mapping:
                if isinstance(updated_mapping[k], dict) and isinstance(v, dict):
                    updated_mapping[k] = _deep_update(updated_mapping[k], v)
                elif isinstance(updated_mapping[k], list) and isinstance(v, list):
                    updated_mapping[k] += v
                else:
                    updated_mapping[k] = v
            else:
                updated_mapping[k] = v
    return updated_mapping
