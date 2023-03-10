"""
Utilities for interracting with Databricks
"""
import io
import logging
import zipfile
from time import sleep
from typing import Any, TypeVar
from urllib.parse import urlparse

import boto3 as boto

from .api.predictions import create_prediction_with_eventlog_bytes, get_prediction
from .api.projects import get_project
from .clients.databricks import get_default_client
from .models import Error, Platform, Preference, Response

logger = logging.getLogger(__name__)


def create_prediction(
    plan_type: str, compute_type: str, eventlog: bytes, project_id: str = None
) -> Response[str]:
    return create_prediction_with_eventlog_bytes(
        Platform.DATABRICKS,
        {"plan_type": plan_type, "compute_type": compute_type},
        "eventlog.zip",
        eventlog,
        project_id,
    )


def create_cluster(config: dict) -> Response[str]:
    if response := get_default_client().create_cluster(config):
        return Response(result=response["cluster_id"])
    return Response(error=Error(code="Databricks Error", message="Failed to create cluster"))


def get_cluster(cluster_id: str) -> Response[dict]:
    if cluster := get_default_client().get_cluster(cluster_id):
        return Response(result=cluster)
    return Response(error=Error(code="Databricks Error", message="Failed to get cluster"))


def create_prediction_for_run(
    run_id: str, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    if run := get_default_client().get_run(run_id):
        if run["state"].get("result_state") != "SUCCESS":
            return Response(
                error=Error(code="Databricks Error", message="Run did not successfully complete")
            )

        cluster_response = _get_job_cluster(run["tasks"], run.get("job_clusters", []))
        if cluster := cluster_response.result:
            eventlog_response = _get_eventlog(run["tasks"][0], cluster)
            if eventlog := eventlog_response.result:
                return create_prediction(plan_type, compute_type, eventlog, project_id)
            return eventlog_response

        return cluster_response

    return Response(error=Error(code="Databricks Error", message="Run not found"))


def record_run(run_id: str, plan_type: str, compute_type: str, project_id: str) -> Response[str]:
    return create_prediction_for_run(run_id, plan_type, compute_type, project_id)


def get_prediction_job(
    job_id: str, prediction_id: str, preference: str = Preference.BALANCED.value
) -> Response[dict]:
    prediction_response = get_prediction(prediction_id)
    if prediction := prediction_response.result:
        if job := get_default_client().get_job(job_id):
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
            return Response(error=Error(code="Databricks Error", message="No task found in job"))
        return Response(error=Error(code="Databricks Error", message="No job found"))
    return prediction_response


def get_project_job(job_id: str, project_id: str, region: str = None) -> Response[dict]:
    project_response = get_project(project_id)
    if project := project_response.result:
        if job := get_default_client().get_job(job_id):
            job_settings = job["settings"]
            if tasks := job_settings.get("tasks", []):
                cluster_response = _get_job_cluster(tasks, job_settings.get("job_clusters", []))
                if cluster := cluster_response.result:
                    if project.s3_url:
                        cluster["cluster_log_conf"] = {
                            "s3": {
                                "destination": project.s3_url,
                                "enable_encryption": True,
                                "region": region or boto.client("s3").meta.region_name,
                                "canned_acl": "bucket-owner-full-control",
                            }
                        }
                        cluster["custom_tags"] = {
                            **cluster.get("custom_tags", {}),
                            "sync:project-id": project_id,
                        }
                    return Response(result=job)
                return cluster_response
            return Response(error=Error(code="Databricks Error", message="No task found in job"))
        return Response(error=Error(code="Databricks Error", message="No job found"))
    return project_response


def run_job_object(job: dict) -> Response[str]:
    tasks = job["settings"]["tasks"]
    cluster_response = _get_job_cluster(tasks, job["settings"].get("job_clusters", []))

    if cluster := cluster_response.result:
        if len(tasks) == 1:
            tasks[0]["new_cluster"] = cluster
            del tasks[0]["job_cluster_key"]
        else:
            cluster["autotermination_minutes"] = 10  # 10 minutes is the minimum
            if cluster_result := get_default_client().create_cluster(cluster):
                for task in tasks:
                    task["existing_cluster_id"] = cluster_result["cluster_id"]
                    if "new_cluster" in task:
                        del task["new_cluster"]
                    if "job_cluster_key" in task:
                        del task["job_cluster_key"]
            else:
                return Response(
                    error=Error(code="Databricks Error", message="Failed to create cluster")
                )

        if run_result := get_default_client().create_run(
            {"run_name": job["settings"]["name"], "tasks": tasks}
        ):
            return Response(result=run_result["run_id"])
        return Response(error=Error(code="Databricks Error", message="Failed to create run"))
    return cluster_response


def run_prediction(job_id: str, prediction_id: str, prefernce: str) -> Response[str]:
    prediction_job_response = get_prediction_job(job_id, prediction_id, prefernce)
    if prediction_job := prediction_job_response.result:
        return run_job_object(prediction_job)
    return prediction_job_response


def create_run(run: dict) -> Response[str]:
    if response := get_default_client().create_run(run):
        return Response(result=response["run_id"])
    return Response(error=Error(code="Databricks Error", message="Failed to create run"))


def run_and_record_prediction_job(
    job_id: str,
    plan_type: str,
    compute_type: str,
    prediction_id: str,
    project_id: str = None,
    region: str = None,
) -> Response[str]:
    project_job_response = get_prediction_job(job_id, prediction_id, region)
    if project_job := project_job_response.result:
        return create_and_record_run(project_job, plan_type, compute_type, project_id)


def run_and_record_project_job(
    job_id: str, plan_type: str, compute_type: str, project_id: str = None, region: str = None
) -> Response[str]:
    project_job_response = get_project_job(job_id, project_id, region)
    if project_job := project_job_response.result:
        return create_and_record_run(project_job, plan_type, compute_type, project_id)


def run_and_record_job(
    job_id: str, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    if run := get_default_client().create_job_run({"job_id": job_id}):
        run_id = run["run_id"]
        wait_response = wait_for_final_run_status(run_id)
        if result_state := wait_response.result:
            if result_state == "SUCCESS":
                return record_run(run_id, plan_type, compute_type, project_id)
            return Response(
                error=Error(
                    code="Databricks Error", message=f"Unsuccessful result state: {result_state}"
                )
            )
        return wait_response
    return Response(error=Error(code="Databricks Error", message="Failed to create job run"))


def create_and_record_run(
    run: dict, plan_type: str, compute_type: str, project_id: str = None
) -> Response[str]:
    run_response = create_run(run)
    if run_id := run_response.result:
        wait_response = wait_for_final_run_status(run_id)
        if result_state := wait_response.result:
            if result_state == "SUCCESS":
                return record_run(run_id, plan_type, compute_type, project_id)
            return Response(
                error=Error(
                    code="Databricks Error", message=f"Unsuccessful result state: {result_state}"
                )
            )
        return wait_response
    return run_response


def create_and_wait_for_run(run: dict) -> Response[str]:
    run_response = create_run(run)
    if run_response.error:
        return run_response

    return wait_for_final_run_status(run_response.result)


def wait_for_final_run_status(run_id: str) -> Response[str]:
    while run := get_default_client().get_run(run_id):
        result_state = run["state"].get("result_state")  # result_state isn't present while running
        if result_state in {"SUCCESS", "FAILED", "TIMEDOUT", "CANCELED"}:
            return Response(result=result_state)
        sleep(30)

    return Response(error=Error(code="Databricks Error", message="Failed to get run"))


def _get_job_cluster(tasks: list[dict], clusters: list) -> Response[dict]:
    if len(tasks) == 1:
        return _get_task_cluster(tasks[0], clusters)

    if [t.get("job_cluster_key") for t in tasks].count(tasks[0].get("job_cluster_key")) == len(
        tasks
    ):
        for cluster in clusters:
            if cluster["job_cluster_key"] == tasks[0].get("job_cluster_key"):
                return Response(result=cluster["new_cluster"])
        return Response(error=Error(code="Databricks Error", message="No cluster found for task"))
    return Response(
        error=Error(code="Databricks Error", message="Not all tasks use the same cluster")
    )


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
            return Response(
                error=Error(code="Databricks Error", message="No cluster found for task")
            )
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

        return Response(error=Error(code="Databricks Error", message="No eventlog found"))


def _get_eventlog_url(task: dict, cluster: dict) -> Response[str]:
    log_url = cluster.get("cluster_log_conf", {}).get("s3", {}).get("destination")
    if log_url:
        parsed_log_url = urlparse(log_url)

        s3 = boto.client("s3")
        contents = s3.list_objects_v2(
            Bucket=parsed_log_url.netloc,
            Prefix=f"{parsed_log_url.path.strip('/')}/{task.get('cluster_instance', {}).get('cluster_id')}/eventlog/{task.get('cluster_instance').get('cluster_id')}",
        ).get("Contents")

        if contents:
            if len(contents) > 1:
                return Response(
                    error=Error(code="Databricks Error", message="More than 1 eventlog found")
                )

            return Response(result=f"s3://{parsed_log_url.netloc}/{contents[0]['Key']}")
        return Response(error=Error(code="Databricks Error", message="No eventlog found"))


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
