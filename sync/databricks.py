"""
Utilities for interracting with Databricks
"""
import logging
from typing import Any, TypeVar
from urllib.parse import urlparse

import boto3 as boto

from .api.predictions import get_prediction, initiate_prediction
from .clients.databricks import get_default_client
from .models import Error, Platform, Preference, Response

logger = logging.getLogger(__name__)


def create_prediction(
    plan_type: str, compute_type: str, eventlog_url: str, project_id: str = None
) -> Response[str]:
    return initiate_prediction(
        Platform.DATABRICKS,
        {"plan_type": plan_type, "compute_type": compute_type},
        eventlog_url,
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


def record_run(run_id: str, plan_type: str, compute_type: str, project_id: str) -> Response[str]:
    if run := get_default_client().get_run(run_id):
        if tasks := run.get("tasks", []):
            if len(tasks) > 1:
                logger.warning("Job has more than 1 task. Proceeding with the first")
            task = tasks[0]

        cluster_response = _get_cluster(task, run.get("job_clusters", []))

        if cluster := cluster_response.result:
            eventlog_response = _get_eventlog_url(task, cluster)
            if eventlog_url := eventlog_response.result:
                return initiate_prediction(
                    Platform.DATABRICKS,
                    {"plan_type": plan_type, "compute_type": compute_type},
                    eventlog_url,
                    project_id,
                )
            return eventlog_response

        return cluster_response

    return Response(error=Error(code="Databricks Error", message="Run not found"))


def get_prediction_run(
    job_id: str, prediction_id: str, preference: str = Preference.BALANCED.value
) -> Response[dict]:
    prediction_response = get_prediction(prediction_id)
    if prediction := prediction_response.result:
        if job := get_default_client().get_job(job_id):
            job_settings = job["settings"]
            if tasks := job_settings.get("tasks", []):
                if len(tasks) > 1:
                    logger.warning("Job has more than 1 task. Proceeding with the first")
                task = tasks[0]

                cluster_response = _get_cluster(task, job_settings.get("job_clusters", []))
                if cluster := cluster_response.result:
                    task["new_cluster"] = _deep_update(
                        cluster, prediction["solutions"][preference]["configuration"]
                    )
                    del task["job_cluster_key"]

                    return Response(result={"tasks": [task]})

                return cluster_response
            return Response(error=Error(code="Databricks Error", message="No task found in job"))
        return Response(error=Error(code="Databricks Error", message="No job found"))
    return prediction_response


def _get_cluster(task: dict, clusters: list) -> Response[dict]:
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
