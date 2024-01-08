"""
Utilities for interacting with Databricks
"""
import gzip
import io
import logging
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Collection, Dict, List, Set, Tuple, TypeVar, Union
from urllib.parse import urlparse

import boto3 as boto

from sync.api.predictions import (
    create_prediction_with_eventlog_bytes,
    get_prediction,
    get_predictions,
    wait_for_final_prediction_status,
)
from sync.api.projects import (
    create_project_recommendation,
    create_project_submission_with_eventlog_bytes,
    get_project,
    get_project_recommendation,
    wait_for_recommendation,
)
from sync.clients.databricks import get_default_client
from sync.config import CONFIG
from sync.models import (
    DatabricksAPIError,
    DatabricksClusterReport,
    DatabricksError,
    PredictionError,
    Response,
)
from sync.utils.dbfs import format_dbfs_filepath, read_dbfs_file

logger = logging.getLogger(__name__)


def create_prediction(
    plan_type: str,
    compute_type: str,
    cluster: dict,
    cluster_events: dict,
    eventlog: bytes,
    instances: dict = None,
    instance_timelines: dict = None,
    volumes: dict = None,
    tasks: List[dict] = None,
    project_id: str = None,
) -> Response[str]:
    """Create a Databricks prediction

    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param cluster: The Databricks cluster definition as defined by -
        https://docs.databricks.com/dev-tools/api/latest/clusters.html#get
    :type cluster: dict
    :param cluster_events: All events, including paginated events, for the cluster as defined by -
        https://docs.databricks.com/dev-tools/api/latest/clusters.html#events
        If the cluster is a long-running cluster, this should only include events relevant to the time window that a
        run occurred in.
    :type cluster_events: dict
    :param eventlog: encoded event log zip
    :type eventlog: bytes
    :param instances: All EC2 Instances that were a part of the cluster. Expects a data format as is returned by
        `boto3's EC2.describe_instances API <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/client/describe_instances.html>`_
        Instances should be narrowed to just those instances relevant to the Databricks Run. This can be done by passing
        a `tag:ClusterId` filter to the describe_instances call like -
        ``Filters=[{"Name": "tag:ClusterId", "Values": ["my-dbx-clusterid"]}]``
        If there are multiple pages of instances, all pages should be accumulated into 1 dictionary and passed to this
        function
    :type instances: dict, optional
    :param volumes: The EBS volumes that were attached to this cluster
    :type volumes: dict, optional
    :param tasks: The Databricks Tasks associated with the cluster
    :type tasks: List[dict]
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    return create_prediction_with_eventlog_bytes(
        get_default_client().get_platform(),
        {
            "plan_type": plan_type,
            "compute_type": compute_type,
            "cluster": cluster,
            "cluster_events": cluster_events,
            "instances": instances,
            "instance_timelines": instance_timelines,
            "volumes": volumes,
            "tasks": tasks,
        },
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
    run_id: str,
    plan_type: str,
    compute_type: str,
    project_id: str = None,
    allow_incomplete_cluster_report: bool = False,
    exclude_tasks: Union[Collection[str], None] = None,
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
    :param allow_incomplete_cluster_report: Whether creating a prediction with incomplete cluster report data should be allowable
    :type allow_incomplete_cluster_report: bool, optional, defaults to False
    :param exclude_tasks: Keys of tasks (task names) to exclude from the prediction
    :type exclude_tasks: Collection[str], optional, defaults to None
    :return: prediction ID
    :rtype: Response[str]
    """
    run = get_default_client().get_run(run_id)

    if "error_code" in run:
        return Response(error=DatabricksAPIError(**run))

    cluster_path = None
    if project_id:
        project_response = get_project(project_id)
        if project_response.error:
            return project_response
        cluster_path = project_response.result.get("cluster_path")

    project_cluster_tasks = _get_project_cluster_tasks(run, project_id, cluster_path, exclude_tasks)

    cluster_tasks = None
    if project_id:
        cluster_tasks = project_cluster_tasks.get(project_id)
    elif len(project_cluster_tasks) == 1:
        cluster_tasks = next(iter(project_cluster_tasks.values()))

    if not cluster_tasks:
        return Response(
            error=DatabricksError(
                message=f"Failed to locate cluster in run {run_id} for project {project_id}"
            )
        )

    cluster_id, tasks = cluster_tasks

    return _create_prediction(
        cluster_id, tasks, plan_type, compute_type, project_id, allow_incomplete_cluster_report
    )


def _create_prediction(
    cluster_id: str,
    tasks: List[dict],
    plan_type: str,
    compute_type: str,
    project_id: str = None,
    allow_incomplete_cluster_report: bool = False,
):
    run_information_response = _get_run_information(
        cluster_id,
        tasks,
        plan_type,
        compute_type,
        allow_incomplete_cluster_report=allow_incomplete_cluster_report,
    )

    if run_information_response.error:
        return run_information_response

    cluster_report, eventlog = run_information_response.result
    return create_prediction(
        **cluster_report.dict(exclude_none=True),
        eventlog=eventlog,
        project_id=project_id,
    )


def create_submission_for_run(
    run_id: str,
    plan_type: str,
    compute_type: str,
    project_id: str,
    allow_incomplete_cluster_report: bool = False,
    exclude_tasks: Union[Collection[str], None] = None,
) -> Response[str]:
    """Create a Submission for the specified Databricks run.

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :param allow_incomplete_cluster_report: Whether creating a prediction with incomplete cluster report data should be allowable
    :type allow_incomplete_cluster_report: bool, optional, defaults to False
    :param exclude_tasks: Keys of tasks (task names) to exclude from the prediction
    :type exclude_tasks: Collection[str], optional, defaults to None
    :return: prediction ID
    :rtype: Response[str]
    """
    run = get_default_client().get_run(run_id)

    if "error_code" in run:
        return Response(error=DatabricksAPIError(**run))

    project_response = get_project(project_id)
    if project_response.error:
        return project_response
    cluster_path = project_response.result.get("cluster_path")

    project_cluster_tasks = _get_project_cluster_tasks(run, project_id, cluster_path, exclude_tasks)

    cluster_tasks = project_cluster_tasks.get(project_id)
    if not cluster_tasks:
        return Response(
            error=DatabricksError(
                message=f"Failed to locate cluster in run {run_id} for project {project_id}"
            )
        )

    cluster_id, tasks = cluster_tasks

    return _create_submission(
        cluster_id, tasks, plan_type, compute_type, project_id, allow_incomplete_cluster_report
    )


def _create_submission(
    cluster_id: str,
    tasks: List[dict],
    plan_type: str,
    compute_type: str,
    project_id: str,
    allow_incomplete_cluster_report: bool = False,
) -> Response[str]:
    run_information_response = _get_run_information(
        cluster_id,
        tasks,
        plan_type,
        compute_type,
        allow_failed_tasks=True,
        allow_incomplete_cluster_report=allow_incomplete_cluster_report,
    )

    if run_information_response.error:
        return run_information_response

    cluster_report, eventlog = run_information_response.result
    return create_project_submission_with_eventlog_bytes(
        get_default_client().get_platform(),
        cluster_report.dict(exclude_none=True),
        "eventlog.zip",
        eventlog,
        project_id,
    )


def _get_run_information(
    cluster_id: str,
    tasks: List[dict],
    plan_type: str,
    compute_type: str,
    allow_failed_tasks: bool = False,
    allow_incomplete_cluster_report: bool = False,
) -> Response[Tuple[DatabricksClusterReport, bytes]]:
    if not allow_failed_tasks and any(
        task["state"].get("result_state") != "SUCCESS" for task in tasks
    ):
        return Response(error=DatabricksError(message="Tasks did not complete successfully"))

    # Making these calls prior to fetching the event log allows Databricks a little extra time to finish
    #  uploading all the event log data before we start checking for it
    cluster_report_response = _get_cluster_report(
        cluster_id, tasks, plan_type, compute_type, allow_incomplete_cluster_report
    )

    cluster_report = cluster_report_response.result
    if cluster_report:
        cluster = cluster_report.cluster
        spark_context_id = _get_run_spark_context_id(tasks)
        end_time = max(task["end_time"] for task in tasks)
        eventlog_response = _get_eventlog(cluster, spark_context_id.result, end_time)

        eventlog = eventlog_response.result
        if eventlog:
            # TODO - allow submissions w/out eventlog. Best way to make eventlog optional?..
            return Response(result=(cluster_report, eventlog))

        return eventlog_response
    return cluster_report_response


def get_cluster_report(
    run_id: str,
    plan_type: str,
    compute_type: str,
    project_id: str = None,
    allow_incomplete: bool = False,
    exclude_tasks: Union[Collection[str], None] = None,
) -> Response[DatabricksClusterReport]:
    """Fetches the cluster information required to create a Sync prediction

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: Databricks Pricing Plan, e.g. "Standard"
    :type plan_type: str
    :param compute_type: Cluster compute type, e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: The Sync Project ID this report should be generated for. This is good to provide in general, but
        especially for multi-cluster jobs.
    :type project_id: str, optional
    :param allow_incomplete: Whether creating a cluster report with incomplete data should be allowable
    :type allow_incomplete: bool, optional, defaults to False
    :param exclude_tasks: Keys of tasks (task names) to exclude from the report
    :type exclude_tasks: Collection[str], optional, defaults to None
    :return: cluster report
    :rtype: Response[DatabricksClusterReport]
    """
    run = get_default_client().get_run(run_id)
    if "error_code" in run:
        return Response(error=DatabricksAPIError(**run))

    cluster_path = None
    if project_id:
        project_response = get_project(project_id)
        if project_response.error:
            return project_response
        cluster_path = project_response.result.get("cluster_path")

    project_cluster_tasks = _get_project_cluster_tasks(run, project_id, cluster_path, exclude_tasks)

    cluster_tasks = None
    if project_id:
        cluster_tasks = project_cluster_tasks.get(project_id)
    elif len(project_cluster_tasks) == 1:
        cluster_tasks = next(iter(project_cluster_tasks.values()))

    if not cluster_tasks:
        return Response(
            error=DatabricksError(
                message=f"Failed to locate cluster in run {run_id} for project {project_id}"
            )
        )

    cluster_id, tasks = cluster_tasks

    return _get_cluster_report(cluster_id, tasks, plan_type, compute_type, allow_incomplete)


def _get_cluster_report(
    cluster_id: str,
    cluster_tasks: List[dict],
    plan_type: str,
    compute_type: str,
    allow_incomplete: bool,
) -> Response[DatabricksClusterReport]:
    raise NotImplementedError()


def _get_cluster_instances_from_dbfs(filepath: str):
    filepath = format_dbfs_filepath(filepath)
    dbx_client = get_default_client()
    try:
        return read_dbfs_file(filepath, dbx_client)
    except Exception as ex:
        logger.error(f"Unexpected error encountered while attempting to fetch {filepath}: {ex}")


def handle_successful_job_run(
    job_id: str,
    run_id: str,
    plan_type: str,
    compute_type: str,
    project_id: Union[str, None] = None,
    allow_incomplete_cluster_report: bool = False,
    exclude_tasks: Union[Collection[str], None] = None,
) -> Response[Dict[str, str]]:
    """Create's Sync project submissions for each eligible cluster in the run (see :py:func:`~record_run`)

    If project ID is provided only submit run data for the cluster tagged with it, or the only cluster if there is such.
    If no project ID is provided then submit run data for each cluster tagged with a project ID.

    For projects with auto_apply_recs=True, apply latest recommended cluster configurations.

    :param job_id: Databricks job ID
    :type job_id: str
    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID
    :type project_id: str, optional, defaults to None
    :param allow_incomplete_cluster_report: Whether creating a prediction with incomplete cluster report data should be allowable
    :type allow_incomplete_cluster_report: bool, optional, defaults to False
    :param exclude_tasks: Keys of tasks (task names) to exclude
    :type exclude_tasks: Collection[str], optional, defaults to None
    :return: map of project ID to submission or prediction ID
    :rtype: Response[Dict[str, str]]
    """
    submission_response = record_run(
        run_id, plan_type, compute_type, project_id, allow_incomplete_cluster_report, exclude_tasks
    )

    if submission_response.error:
        return submission_response

    for project_id, submission_id in submission_response.result.items():
        project_response = get_project(project_id)

        if project_response.error:
            logger.error(f"Failed to retrieve project {project_id} - {project_response.error}")
            continue

        project = project_response.result
        if project["auto_apply_recs"]:
            if project["project_model_id"] in {"GRADIENT_ML", "UNASSIGNED"}:
                recommendation_response = create_and_apply_project_recommendation(
                    project_id, job_id
                )
                if recommendation_response.error:
                    logger.error(
                        f"Failed to create and apply project {project_id} recommendation to job {job_id} - {recommendation_response.error}"
                    )
            elif project["project_model_id"] == "AUTOTUNER":
                prediction_response = wait_for_and_apply_prediction(
                    project_id, submission_id, job_id
                )
                if prediction_response.error:
                    logger.error(
                        f"Failed to apply prediction {submission_id} to job {job_id} - {prediction_response.error}"
                    )
            else:
                logger.error(
                    f"Unexpected project_model_id for project {project_id}: {project['project_model_id']}"
                )

    return submission_response


def create_and_apply_project_recommendation(project_id: str, job_id: str) -> Response[str]:
    """Create recommendation for project and apply it to the job

    :param job_id: ID of job to which the recommendation should be applied
    :type job_id: str
    :param project_id: ID of project for job
    :type project_id: str
    :return: ID of applied recommendation
    :rtype: Response[str]
    """
    recommendation_response = create_project_recommendation(project_id)

    if recommendation_response.error:
        return recommendation_response

    recommendation_id = recommendation_response.result

    recommendation_wait_response = wait_for_recommendation(project_id, recommendation_id)

    if recommendation_wait_response.error:
        return recommendation_wait_response

    return apply_project_recommendation(job_id, project_id, recommendation_id)


def wait_for_and_apply_prediction(
    project_id: str, prediction_id: str, job_id: str
) -> Response[str]:
    """Wait for prediction and apply it to the job

    :param project_id: ID of project for job
    :type project_id: str
    :param prediction_id: ID of project for job
    :type prediction_id: str
    :param job_id: ID of job to which the recommendation should be applied
    :type job_id: str
    :return: ID of applied recommendation
    :rtype: Response[str]
    """
    prediction_status_response = wait_for_final_prediction_status(prediction_id)

    if prediction_status_response.error:
        return prediction_status_response

    prediction_status = prediction_status_response.result
    if prediction_status == "SUCCESS":
        return apply_prediction(job_id, project_id, prediction_id)

    return Response(
        error=PredictionError(f"Prediction {prediction_id} failed. Status: {prediction_status}")
    )


def record_run(
    run_id: str,
    plan_type: str,
    compute_type: str,
    project_id: Union[str, None] = None,
    allow_incomplete_cluster_report: bool = False,
    exclude_tasks: Union[Collection[str], None] = None,
) -> Response[Dict[str, str]]:
    """Create's Sync project submissions for each eligible cluster in the run.

    If project ID is provided only submit run data for the cluster tagged with it, or the only cluster if there is such.
    If no project ID is provided then submit run data for each cluster tagged with a project ID.

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID
    :type project_id: str, optional, defaults to None
    :param allow_incomplete_cluster_report: Whether creating a prediction with incomplete cluster report data should be allowable
    :type allow_incomplete_cluster_report: bool, optional, defaults to False
    :param exclude_tasks: Keys of tasks (task names) to exclude
    :type exclude_tasks: Collection[str], optional, defaults to None
    :return: map of project ID to submission or prediction ID
    :rtype: Response[Dict[str, str]]
    """
    run = get_default_client().get_run(run_id)

    if "error_code" in run:
        return Response(error=DatabricksAPIError(**run))

    cluster_path = None
    if project_id:
        project_response = get_project(project_id)
        if project_response.error:
            return project_response
        cluster_path = project_response.result.get("cluster_path")

    project_cluster_tasks = _get_project_cluster_tasks(run, project_id, cluster_path, exclude_tasks)

    if not project_cluster_tasks:
        return Response(
            error=DatabricksError(
                message=f"Failed to locate cluster in run {run_id} for project {project_id}"
            )
        )

    result_ids = _record_project_clusters(
        project_cluster_tasks, plan_type, compute_type, allow_incomplete_cluster_report
    )

    if result_ids:
        return Response(result=result_ids)

    return Response(error=DatabricksError(message=f"Failed to submit run {run_id} to any projects"))


def _record_project_clusters(
    project_cluster_tasks: Dict[str, Tuple[str, List[dict]]],
    plan_type: str,
    compute_type: str,
    allow_incomplete_cluster_report: bool,
) -> Dict[str, str]:
    """Creates project submissions/predictions and returns a map of project IDs to the new submissions/predictions IDs"""
    result_ids = {}
    for cluster_project_id, (cluster_id, tasks) in project_cluster_tasks.items():
        project_response = get_project(cluster_project_id)

        if project_response.error:
            logger.error(
                f"Failed to retrieve project {cluster_project_id} - {project_response.error}"
            )
            continue

        project = project_response.result

        if project["project_model_id"] in {"GRADIENT_ML", "UNASSIGNED"}:
            submission_response = _create_submission(
                cluster_id,
                tasks,
                plan_type,
                compute_type,
                cluster_project_id,
                allow_incomplete_cluster_report,
            )
        elif project["project_model_id"] == "AUTOTUNER":
            submission_response = _create_prediction(
                cluster_id,
                tasks,
                plan_type,
                compute_type,
                cluster_project_id,
                allow_incomplete_cluster_report,
            )
        else:
            logger.error(
                f"Unexpected project_model_id for project {cluster_project_id}: {project['project_model_id']}"
            )
            continue

        submission_id = submission_response.result
        if submission_id:
            result_ids[cluster_project_id] = submission_id
        else:
            logger.error(
                f"Failed to submit run data for cluster {cluster_id} in project {cluster_project_id} - {submission_response.error}"
            )

    return result_ids


def apply_prediction(
    job_id: str, project_id: str, prediction_id: str = None, preference: str = None
):
    """Updates jobs with prediction configuration

    :param job_id: ID of job to apply prediction to
    :type job_id: str
    :param project_id: Sync project ID
    :type project_id: str
    :param prediction_id: Sync prediction ID, defaults to latest in project
    :type prediction_id: str, optional
    :param preference: Prediction preference, defaults to "recommended" then "economy"
    :type preference: str, optional
    :return: ID of applied prediction
    :rtype: Response[str]
    """
    if prediction_id:
        prediction_response = get_prediction(prediction_id, preference)
    else:
        predictions_response = get_predictions(project_id=project_id)
        if predictions_response.error:
            return predictions_response
        prediction_id = predictions_response.result[0]["prediction_id"]
        prediction_response = get_prediction(prediction_id, preference)

    if prediction_response.error:
        return prediction_response

    prediction = prediction_response.result

    databricks_client = get_default_client()

    job = databricks_client.get_job(job_id)
    job_clusters = _get_project_job_clusters(job)

    project_cluster = job_clusters.get(project_id)
    if not project_cluster:
        if len(job_clusters) == 1:
            project_cluster = next(iter(job_clusters.values()))
        else:
            return Response(
                error=DatabricksError(
                    message=f"Failed to locate cluster in job {job_id} for project {project_id}"
                )
            )

    project_cluster_path, _ = project_cluster

    if preference:
        prediction_cluster = prediction["solutions"][preference]["configuration"]
    else:
        prediction_cluster = prediction["solutions"].get(
            "recommended", prediction["solutions"]["economy"]
        )["configuration"]

    if "cluster_name" in prediction_cluster:
        del prediction_cluster["cluster_name"]

    if project_cluster_path[0] == "job_clusters":
        new_settings = {
            "job_clusters": [
                {"job_cluster_key": project_cluster_path[1], "new_cluster": prediction_cluster}
            ]
        }
    else:
        new_settings = {
            "tasks": [{"task_key": project_cluster_path[1], "new_cluster": prediction_cluster}]
        }

    response = databricks_client.update_job(job_id, new_settings)

    if "error_code" in response:
        return Response(error=DatabricksAPIError(**response))

    return Response(result=prediction_id)


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
    job = get_default_client().get_job(job_id)
    if "error_code" in job:
        return Response(error=DatabricksAPIError(**job))

    job_settings = job["settings"]
    tasks = job_settings.get("tasks", [])
    if tasks:
        cluster_response = _get_job_cluster(tasks, job_settings.get("job_clusters", []))
        cluster = cluster_response.result
        if cluster:
            prediction_cluster_response = get_prediction_cluster(cluster, prediction_id, preference)
            prediction_cluster = prediction_cluster_response.result
            if prediction_cluster:
                cluster_key = tasks[0].get("job_cluster_key")
                if cluster_key:
                    job_settings["job_clusters"] = [
                        j
                        for j in job_settings["job_clusters"]
                        if j.get("job_cluster_key") != cluster_key
                    ] + [{"job_cluster_key": cluster_key, "new_cluster": prediction_cluster}]
                else:
                    # For `new_cluster` definitions, Databricks will automatically assign the newly created cluster a name,
                    # and will reject any run submissions where the `cluster_name` is pre-populated
                    if "cluster_name" in prediction_cluster:
                        del prediction_cluster["cluster_name"]
                    tasks[0]["new_cluster"] = prediction_cluster
                return Response(result=job)
            return prediction_cluster_response
        return cluster_response
    return Response(error=DatabricksError(message="No task found in job"))


def get_prediction_cluster(
    cluster: dict, prediction_id: str, preference: str = CONFIG.default_prediction_preference.value
) -> Response[dict]:
    """Apply the prediction to the provided cluster.

    The cluster is updated with configuration from the prediction and returned in the result.

    :param cluster: Databricks cluster object
    :type cluster: dict
    :param prediction_id: prediction ID
    :type prediction_id: str
    :param preference: preferred prediction solution, defaults to local configuration
    :type preference: str, optional
    :return: job object with prediction applied to it
    :rtype: Response[dict]
    """
    prediction_response = get_prediction(prediction_id)
    prediction = prediction_response.result
    if prediction:
        # num_workers/autoscale are mutually exclusive settings, and we are relying on our Prediction
        #  Recommendations to set these appropriately. Since we may recommend a Static cluster (i.e. a cluster
        #  with `num_workers`) for a cluster that was originally autoscaled, we want to make sure to remove this
        #  prior configuration
        if "num_workers" in cluster:
            del cluster["num_workers"]

        if "autoscale" in cluster:
            del cluster["autoscale"]

        prediction_cluster = _deep_update(
            cluster, prediction["solutions"][preference]["configuration"]
        )

        return Response(result=prediction_cluster)
    return prediction_response


def apply_project_recommendation(
    job_id: str, project_id: str, recommendation_id: str
) -> Response[str]:
    """Updates jobs with project recommendation

    :param job_id: ID of job to apply prediction to
    :type job_id: str
    :param project_id: Sync project ID
    :type project_id: str
    :param recommendation_id: Sync project recommendation ID
    :type recommendation_id: str
    :return: ID of applied recommendation
    :rtype: Response[str]
    """
    databricks_client = get_default_client()

    job = databricks_client.get_job(job_id)
    job_clusters = _get_project_job_clusters(job)

    project_cluster = job_clusters.get(project_id)
    if not project_cluster:
        if len(job_clusters) == 1:
            project_cluster = next(iter(job_clusters.values()))
        else:
            return Response(
                error=DatabricksError(
                    message=f"Failed to locate cluster in job {job_id} for project {project_id}"
                )
            )

    project_cluster_path, project_cluster_def = project_cluster

    new_cluster_def_response = get_recommendation_cluster(
        project_cluster_def, project_id, recommendation_id
    )
    if new_cluster_def_response.error:
        return new_cluster_def_response
    new_cluster_def = new_cluster_def_response.result

    if project_cluster_path[0] == "job_clusters":
        new_settings = {
            "job_clusters": [
                {"job_cluster_key": project_cluster_path[1], "new_cluster": new_cluster_def}
            ]
        }
    else:
        new_settings = {
            "tasks": [{"task_key": project_cluster_path[1], "new_cluster": new_cluster_def}]
        }

    response = databricks_client.update_job(job_id, new_settings)

    if "error_code" in response:
        return Response(error=DatabricksAPIError(**response))

    return Response(result=recommendation_id)


def get_recommendation_job(job_id: str, project_id: str, recommendation_id: str) -> Response[dict]:
    """Apply the recommendation to the specified job.

    The basis job can only have tasks that run on the same cluster. That cluster is updated with the
    configuration from the prediction and returned in the result job configuration. Use this function
    to apply a prediction to an existing job or test a prediction with a one-off run.

    :param job_id: basis job ID
    :type job_id: str
    :param project_id: Sync project ID
    :type project_id: str
    :param recommendation_id: recommendation ID
    :type recommendation_id: str
    :return: job object with recommendation applied to it
    :rtype: Response[dict]
    """
    job = get_default_client().get_job(job_id)

    if "error_code" in job:
        return Response(error=DatabricksAPIError(**job))

    job_settings = job["settings"]
    tasks = job_settings.get("tasks", [])
    if tasks:
        cluster_response = _get_job_cluster(tasks, job_settings.get("job_clusters", []))
        cluster = cluster_response.result
        if cluster:
            recommendation_cluster_response = get_recommendation_cluster(
                cluster, project_id, recommendation_id
            )
            recommendation_cluster = recommendation_cluster_response.result
            if recommendation_cluster:
                cluster_key = tasks[0].get("job_cluster_key")
                if cluster_key:
                    job_settings["job_clusters"] = [
                        j
                        for j in job_settings["job_clusters"]
                        if j.get("job_cluster_key") != cluster_key
                    ] + [{"job_cluster_key": cluster_key, "new_cluster": recommendation_cluster}]
                else:
                    # For `new_cluster` definitions, Databricks will automatically assign the newly created cluster a name,
                    # and will reject any run submissions where the `cluster_name` is pre-populated
                    if "cluster_name" in recommendation_cluster:
                        del recommendation_cluster["cluster_name"]
                    tasks[0]["new_cluster"] = recommendation_cluster
                return Response(result=job)
            return recommendation_cluster_response
        return cluster_response
    return Response(error=DatabricksError(message="No task found in job"))


def get_recommendation_cluster(
    cluster: dict, project_id: str, recommendation_id: str
) -> Response[dict]:
    """Apply the recommendation to the provided cluster.

    The cluster is updated with configuration from the prediction and returned in the result.

    :param cluster: Databricks cluster object
    :type cluster: dict
    :param project_id: Sync project ID
    :type project_id: str
    :param recommendation_id: The id of the recommendation to fetch and apply to the given cluster
    :type recommendation_id: str, optional
    :return: cluster object with prediction applied to it
    :rtype: Response[dict]
    """
    recommendation_response = get_project_recommendation(project_id, recommendation_id)
    recommendation = recommendation_response.result.get("recommendation")
    if recommendation:
        # num_workers/autoscale are mutually exclusive settings, and we are relying on our Prediction
        #  Recommendations to set these appropriately. Since we may recommend a Static cluster (i.e. a cluster
        #  with `num_workers`) for a cluster that was originally autoscaled, we want to make sure to remove this
        #  prior configuration
        if "num_workers" in cluster:
            del cluster["num_workers"]

        if "autoscale" in cluster:
            del cluster["autoscale"]

        recommendation_cluster = _deep_update(cluster, recommendation["configuration"])

        return Response(result=recommendation_cluster)
    return recommendation_response


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
    tasks = job_settings.get("tasks", [])
    if tasks:
        cluster_response = _get_job_cluster(tasks, job_settings.get("job_clusters", []))
        cluster = cluster_response.result
        if cluster:
            project_cluster_response = get_project_cluster(cluster, project_id, region_name)
            project_cluster = project_cluster_response.result
            if project_cluster:
                cluster_key = tasks[0].get("job_cluster_key")
                if cluster_key:
                    job_settings["job_clusters"] = [
                        j
                        for j in job_settings["job_clusters"]
                        if j.get("job_cluster_key") != cluster_key
                    ] + [{"job_cluster_key": cluster_key, "new_cluster": project_cluster}]
                else:
                    tasks[0]["new_cluster"] = project_cluster

                return Response(result=job)
            return project_cluster_response
        return cluster_response
    return Response(error=DatabricksError(message="No task found in job"))


def get_project_cluster(cluster: dict, project_id: str, region_name: str = None) -> Response[dict]:
    """Apply project configuration to a cluster.

    The cluster is updated with tags and a log configuration to facilitate project continuity.

    :param cluster: Databricks cluster object
    :type cluster: dict
    :param project_id: Sync project ID
    :type project_id: str
    :param region_name: region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: project job object
    :rtype: Response[dict]
    """
    project_settings_response = get_project_cluster_settings(project_id, region_name)
    project_cluster_settings = project_settings_response.result
    if project_cluster_settings:
        project_cluster = _deep_update(cluster, project_cluster_settings)

        return Response(result=project_cluster)
    return project_settings_response


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
    project = project_response.result
    if project:
        result = {
            "custom_tags": {
                "sync:project-id": project_id,
            }
        }

        cluster_log_url = urlparse(project.get("cluster_log_url"))
        if cluster_log_url.scheme == "s3":
            result.update(
                {
                    "cluster_log_conf": {
                        "s3": {
                            "destination": f"{cluster_log_url.geturl()}/{project_id}",
                            "enable_encryption": True,
                            "region": region_name or boto.client("s3").meta.region_name,
                            "canned_acl": "bucket-owner-full-control",
                        }
                    }
                }
            )

        elif cluster_log_url.scheme == "dbfs":
            result.update(
                {
                    "cluster_log_conf": {
                        "dbfs": {
                            "destination": f"{cluster_log_url.geturl()}/{project_id}",
                        }
                    }
                }
            )

        return Response(result=result)
    return project_response


def run_job_object(job: dict) -> Response[Tuple[str, str]]:
    """Create a Databricks one-off run based on the job configuration.

    :param job: Databricks job object
    :type job: dict
    :return: run ID, and optionally ID of newly created cluster
    :rtype: Response[Tuple[str, str]]
    """
    tasks = job["settings"]["tasks"]
    cluster_response = _get_job_cluster(tasks, job["settings"].get("job_clusters", []))

    cluster = cluster_response.result
    if cluster:
        new_cluster_id = None
        if len(tasks) == 1:
            # For `new_cluster` definitions, Databricks will automatically assign the newly created cluster a name,
            #  and will reject any run submissions where the `cluster_name` is pre-populated
            if "cluster_name" in cluster:
                del cluster["cluster_name"]

            tasks[0]["new_cluster"] = cluster
            del tasks[0]["job_cluster_key"]
        else:
            # If the original Job has a pre-existing Policy, we want to remove this from the `create_cluster` payload,
            #  since we are not allowed to create clusters with certain policies via that endpoint, e.g. we cannot
            #  create a `Job Compute` cluster via this endpoint.
            if "policy_id" in cluster:
                del cluster["policy_id"]

            # Create an "All-Purpose Compute" cluster
            cluster["cluster_name"] = cluster["cluster_name"] or job["settings"]["name"]
            cluster["autotermination_minutes"] = 10  # 10 minutes is the minimum

            cluster_result = get_default_client().create_cluster(cluster)
            if "error_code" in cluster_result:
                return Response(error=DatabricksAPIError(**cluster_result))

            new_cluster_id = cluster_result["cluster_id"]

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

        return Response(result=(run_result["run_id"], new_cluster_id))
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
    prediction_job = prediction_job_response.result
    if prediction_job:
        run_response = run_job_object(prediction_job)
        if run_response.result:
            return Response(result=run_response.result[0])
        return run_response
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
    prediction_job = prediction_job_response.result
    if prediction_job:
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
    project_job = project_job_response.result
    if project_job:
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
    run_and_cluster_ids = run_response.result
    if run_and_cluster_ids:
        response = wait_for_run_and_cluster(run_and_cluster_ids[0])
        result_state = response.result
        if result_state:
            if result_state == "SUCCESS":
                response = record_run(run_and_cluster_ids[0], plan_type, compute_type, project_id)
            else:
                response = Response(
                    error=DatabricksError(message=f"Unsuccessful run result state: {result_state}")
                )

        for cluster_id in run_and_cluster_ids[1:]:
            delete_cluster_response = get_default_client().delete_cluster(cluster_id)
            if "error_code" in delete_cluster_response:
                logger.warning(
                    f"Failed to delete cluster {cluster_id}: {delete_cluster_response['error_code']}: {delete_cluster_response['message']}"
                )

        return response
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
    run_id = run_response.result
    if run_id:
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
    result_state = wait_response.result
    if result_state:
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
            for cluster_id in {task.get("existing_cluster_id") for task in run["tasks"]}:
                cluster_response = terminate_cluster(cluster_id)
                if cluster_response.error:
                    return cluster_response
            return Response(result=result_state)

        sleep(30)
        run = get_default_client().get_run(run_id)

    return Response(error=DatabricksAPIError(**run))


def terminate_cluster(cluster_id: str) -> Response[dict]:
    """Terminate Databricks cluster and wait to return final state.

    :param cluster_id: Databricks cluster ID
    :type cluster_id: str
    :return: Databricks cluster object with state: "TERMINATED"
    :rtype: Response[str]
    """
    cluster = get_default_client().get_cluster(cluster_id)
    if "error_code" not in cluster:
        state = cluster.get("state")
        if state == "TERMINATED":
            return Response(result=cluster)
        elif state == "TERMINATING":
            return _wait_for_cluster_termination(cluster_id)
        elif state in {"PENDING", "RUNNING", "RESTARTING", "RESIZING"}:
            get_default_client().terminate_cluster(cluster_id)
            return _wait_for_cluster_termination(cluster_id)
        else:
            return Response(error=DatabricksError(message=f"Unexpected cluster state: {state}"))

    return Response(error=DatabricksAPIError(**cluster))


def _wait_for_cluster_termination(
    cluster_id: str, timeout_seconds=600, poll_seconds=10
) -> Response[dict]:
    logging.info(f"Waiting for cluster {cluster_id} to terminate")
    start_seconds = time.time()
    cluster = get_default_client().get_cluster(cluster_id)
    while "error_code" not in cluster:
        state = cluster.get("state")
        if state == "TERMINATED":
            return Response(result=cluster)
        elif state == "TERMINATING":
            sleep(poll_seconds)
        else:
            return Response(error=DatabricksError(message=f"Unexpected cluster state: {state}"))

        if time.time() - start_seconds > timeout_seconds:
            return Response(
                error=DatabricksError(
                    message=f"Cluster failed to terminate after waiting {timeout_seconds} seconds"
                )
            )

        cluster = get_default_client().get_cluster(cluster_id)

    return Response(error=DatabricksAPIError(**cluster))


def _cluster_log_destination(
    cluster: dict,
) -> Union[Tuple[str, str, str, str], Tuple[None, None, None, None]]:
    cluster_log_conf = cluster.get("cluster_log_conf", {})
    s3_log_url = cluster_log_conf.get("s3", {}).get("destination")
    dbfs_log_url = cluster_log_conf.get("dbfs", {}).get("destination")

    log_url = s3_log_url or dbfs_log_url

    if log_url:
        parsed_log_url = urlparse(log_url)
        bucket = parsed_log_url.netloc
        stripped_path = parsed_log_url.path.strip("/")

        # If the event log destination is just a *bucket* without any sub-path, then we don't want to include
        #  a leading `/` in our Prefix (which will make it so that we never actually find the event log), so
        #  we make sure to re-strip our final Prefix
        base_cluster_filepath_prefix = f"{stripped_path}/{cluster['cluster_id']}".strip("/")

        return log_url, parsed_log_url.scheme, bucket, base_cluster_filepath_prefix

    return None, None, None, None


def _get_job_cluster(tasks: List[dict], job_clusters: list) -> Response[dict]:
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


def _get_project_job_clusters(
    job: dict,
    exclude_tasks: Union[Collection[str], None] = None,
) -> Dict[str, Tuple[Tuple[str], dict]]:
    """Returns a mapping of project IDs to cluster paths and clusters.

    Cluster paths are tuples that can be used to locate clusters in a job object, e.g.

    ("tasks", <task_key>) or ("job_clusters", <job_cluster_key>)

    Items for project IDs with more than 1 associated cluster are omitted"""
    job_clusters = {
        c["job_cluster_key"]: c["new_cluster"] for c in job["settings"].get("job_clusters", [])
    }
    all_project_clusters = defaultdict(dict)

    for task in job["settings"]["tasks"]:
        if not exclude_tasks or task["task_key"] not in exclude_tasks:
            task_cluster = task.get("new_cluster")
            if task_cluster:
                task_cluster_path = ("tasks", task["task_key"])

            if not task_cluster:
                task_cluster = job_clusters.get(task.get("job_cluster_key"))
                task_cluster_path = ("job_clusters", task.get("job_cluster_key"))

            if task_cluster:
                cluster_project_id = task_cluster.get("custom_tags", {}).get("sync:project-id")
                all_project_clusters[cluster_project_id][task_cluster_path] = task_cluster

    filtered_project_clusters = {}
    for project_id, clusters in all_project_clusters.items():
        if len(clusters) > 1:
            logger.warning(f"More than 1 cluster found for project ID {project_id}")
        else:
            filtered_project_clusters[project_id] = next(iter(clusters.items()))

    return filtered_project_clusters


def _get_project_cluster_tasks(
    run: dict,
    project_id: str = None,
    cluster_path: str = None,
    exclude_tasks: Union[Collection[str], None] = None,
) -> Dict[str, Tuple[str, List[dict]]]:
    """Returns a mapping of project IDs to cluster-ID-tasks pairs"""
    project_cluster_tasks = _get_cluster_tasks(run, exclude_tasks)

    filtered_project_cluster_tasks = {}
    if project_id:
        if project_id in project_cluster_tasks:
            filtered_project_cluster_tasks = {project_id: project_cluster_tasks.get(project_id)}
        elif cluster_path:
            # A cluster can only be tagged with 1 project so this dict will only have 1 item at most
            filtered_project_cluster_tasks = {
                project_id: cluster_tasks
                for _, cluster_tasks in project_cluster_tasks.items()
                if cluster_path in cluster_tasks
            }

        if not filtered_project_cluster_tasks and len(project_cluster_tasks) == 1:
            # If there's only 1 cluster assume that's the one for the project
            filtered_project_cluster_tasks = {
                project_id: next(iter(project_cluster_tasks.values()))
            }

        assert not filtered_project_cluster_tasks or len(filtered_project_cluster_tasks) == 1
    else:
        filtered_project_cluster_tasks = {
            cluster_project_id: cluster_tasks
            for cluster_project_id, cluster_tasks in project_cluster_tasks.items()
            if cluster_project_id and (not cluster_path or cluster_path in cluster_tasks)
        }

    assert None not in filtered_project_cluster_tasks

    result_project_cluster_tasks = {}
    for project_id, cluster_tasks in filtered_project_cluster_tasks.items():
        result_cluster_tasks = None
        if len(cluster_tasks) > 1:  # if 2 clusters are tagged with the same project ID
            if cluster_path in cluster_tasks:
                result_cluster_tasks = cluster_tasks[cluster_path]
        else:
            result_cluster_tasks = next(iter(cluster_tasks.values()))

        if result_cluster_tasks:
            result_project_cluster_tasks[project_id] = result_cluster_tasks

    return result_project_cluster_tasks


def _get_cluster_tasks(
    run: dict,
    exclude_tasks: Union[Collection[str], None] = None,
) -> Dict[str, Dict[str, Tuple[str, List[dict]]]]:
    """Returns a mapping of project IDs to cluster paths to cluster IDs and tasks"""
    job_clusters = {c["job_cluster_key"]: c["new_cluster"] for c in run.get("job_clusters", [])}

    cluster_id_tasks = defaultdict(list)
    cluster_path_ids = defaultdict(set)
    cluster_project_paths = defaultdict(set)

    for task in run["tasks"]:
        if "cluster_instance" in task and (
            not exclude_tasks or task["task_key"] not in exclude_tasks
        ):
            cluster_id = task["cluster_instance"]["cluster_id"]

            task_cluster = task.get("new_cluster")
            if task_cluster:
                cluster_path = f"tasks/{task['task_key']}"
            else:
                task_cluster = job_clusters.get(task.get("job_cluster_key"))
                cluster_path = f"job_clusters/{task.get('job_cluster_key')}"

            if task_cluster:
                cluster_project_id = task_cluster.get("custom_tags", {}).get("sync:project-id")
                cluster_id_tasks[cluster_id].append(task)
                cluster_path_ids[cluster_path].add(cluster_id)
                cluster_project_paths[cluster_project_id].add(cluster_path)

    result_cluster_project_tasks = {}
    for project_id, cluster_paths in cluster_project_paths.items():
        cluster_path_tasks = {}
        for cluster_path in cluster_paths:
            if len(cluster_path_ids[cluster_path]) == 1:
                cluster_id = cluster_path_ids[cluster_path].pop()
                cluster_path_tasks[cluster_path] = (cluster_id, cluster_id_tasks[cluster_id])
            else:
                # Maybe this will happen if the same job cluster is used by 2 non adjacent tasks
                logger.warning(
                    f"More than 1 cluster instance found for the cluster at {cluster_path}"
                )

        if cluster_path_tasks:
            result_cluster_project_tasks[project_id] = cluster_path_tasks

    return result_cluster_project_tasks


def _get_run_spark_context_id(tasks: List[dict]) -> Response[str]:
    context_ids = {
        task["cluster_instance"]["spark_context_id"] for task in tasks if "cluster_instance" in task
    }
    num_ids = len(context_ids)

    if num_ids == 1:
        return Response(result=context_ids.pop())
    elif num_ids == 0:
        return Response(error=DatabricksError(message="No cluster found for tasks"))
    else:
        return Response(error=DatabricksError(message="More than 1 cluster found for tasks"))


def _get_task_cluster(task: dict, clusters: list) -> Response[dict]:
    cluster = task.get("new_cluster")

    if not cluster:
        cluster_matches = [
            candidate
            for candidate in clusters
            if candidate["job_cluster_key"] == task.get("job_cluster_key")
        ]
        if cluster_matches:
            cluster = cluster_matches[0]["new_cluster"]
        else:
            return Response(error=DatabricksError(message="No cluster found for task"))
    return Response(result=cluster)


def _s3_contents_have_all_rollover_logs(contents: List[dict], run_end_time_seconds: float):
    final_rollover_log = contents and next(
        (
            content
            for content in contents
            if Path(content["Key"]).stem in {"eventlog", "eventlog.gz", "eventlog.json.gz"}
        ),
        False,
    )
    return (
        # Related to - https://docs.databricks.com/clusters/configure.html#cluster-log-delivery-1
        # We want to make sure that the final_rollover_log has had data written to it AFTER the run has actually
        # completed. We use this as a signal that Databricks has flushed all event log data to S3, and that we
        # can proceed with the upload + prediction. If this proves to be finicky in some way, we can always
        # wait for the full 5 minutes after the run_end_time_seconds
        final_rollover_log
        and final_rollover_log["LastModified"].timestamp() >= run_end_time_seconds
    )


def _dbfs_directory_has_all_rollover_logs(contents: dict, run_end_time_millis: float):
    files = contents["files"]
    final_rollover_log = contents and next(
        (
            file
            for file in files
            if Path(file["path"]).stem in {"eventlog", "eventlog.gz", "eventlog.json.gz"}
        ),
        False,
    )
    return (
        # Related to - https://docs.databricks.com/clusters/configure.html#cluster-log-delivery-1
        # We want to make sure that the final_rollover_log has had data written to it AFTER the run has actually
        # completed. We use this as a signal that Databricks has flushed all event log data to DBFS, and that we
        # can proceed with the upload + prediction.
        final_rollover_log
        and final_rollover_log["modification_time"] >= run_end_time_millis
    )


def _dbfs_any_file_has_zero_size(dbfs_contents: Dict) -> bool:
    any_zeros = any(file["file_size"] == 0 for file in dbfs_contents["files"])
    if any_zeros:
        logger.info("One or more dbfs event log files has a file size of zero")
    return any_zeros


def _check_total_file_size_changed(
    last_total_file_size: int, dbfs_contents: Dict
) -> Tuple[bool, int]:

    new_total_file_size = sum([file.get("file_size", 0) for file in dbfs_contents.get("files", {})])
    if new_total_file_size == last_total_file_size:
        return False, new_total_file_size
    else:
        logger.info("Total file size of eventlog directory changed")
        return True, new_total_file_size


def _event_log_poll_duration_seconds():
    """Convenience function to aid testing"""
    return 15


def _get_eventlog_from_s3(
    cluster_id: str,
    bucket: str,
    base_filepath: str,
    run_end_time_millis: int,
    poll_duration_seconds: int,
):
    s3 = boto.client("s3")

    # If the event log destination is just a *bucket* without any sub-path, then we don't want to include
    #  a leading `/` in our Prefix (which will make it so that we never actually find the event log), so
    #  we make sure to re-strip our final Prefix
    # TODO - using the spark_context_id might be good here, as we do in the DBFS logic
    prefix = f"{base_filepath}/eventlog/{cluster_id}".strip("/")

    logger.info(f"Looking for eventlogs at location: {prefix}")

    contents = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents")
    run_end_time_seconds = run_end_time_millis / 1000
    poll_num_attempts = 0
    poll_max_attempts = 20  # 5 minutes / 15 seconds = 20 attempts
    while (
        not _s3_contents_have_all_rollover_logs(contents, run_end_time_seconds)
        and poll_num_attempts < poll_max_attempts
    ):
        if poll_num_attempts > 0:
            logger.info(
                f"No or incomplete event log data detected - attempting again in {poll_duration_seconds} seconds"
            )
            sleep(poll_duration_seconds)

        contents = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents")
        poll_num_attempts += 1

    if contents:
        eventlog_zip = io.BytesIO()
        eventlog_zip_file = zipfile.ZipFile(eventlog_zip, "a", zipfile.ZIP_DEFLATED)

        for content in contents:
            obj = s3.get_object(Bucket=bucket, Key=content["Key"])
            eventlog_zip_file.writestr(content["Key"].split("/")[-1], obj["Body"].read())

        eventlog_zip_file.close()

        return Response(result=eventlog_zip.getvalue())

    return Response(
        error=DatabricksError(
            message=f"No eventlog found at location - {bucket}/{base_filepath} - after {poll_num_attempts * poll_duration_seconds} seconds"
        )
    )


def _get_eventlog_from_dbfs(
    cluster_id: str,
    spark_context_id: str,
    base_filepath: str,
    run_end_time_millis: int,
    poll_duration_seconds: int,
):
    dbx_client = get_default_client()

    prefix = format_dbfs_filepath(f"{base_filepath}/eventlog/")
    root_dir = dbx_client.list_dbfs_directory(prefix)
    eventlog_files = [f for f in root_dir["files"] if f["is_dir"]]
    matching_subdirectory = None

    # For shared clusters, we may find multiple subdirectories under the same root path, in the format -
    #     {cluster_id}_{driver_ip_address}
    # Since DBFS gives us no good filtering mechanism for this, and there isn't always an easy way to get
    #  the driver's IP address for a Run, we can list the subdirectories and look for one containing a path
    #  that ends with this cluster's `spark_context_id`
    while eventlog_files and not matching_subdirectory:
        eventlog_file_metadata = eventlog_files.pop()
        path = eventlog_file_metadata["path"]

        subdir = dbx_client.list_dbfs_directory(path)

        subdir_files = subdir["files"]
        matching_subdirectory = next(
            (subfile for subfile in subdir_files if spark_context_id in subfile["path"]),
            None,
        )

    if matching_subdirectory:
        eventlog_dir = dbx_client.list_dbfs_directory(matching_subdirectory["path"])

        poll_num_attempts = 0
        poll_max_attempts = 20  # 5 minutes / 15 seconds = 20 attempts

        total_file_size = 0
        file_size_changed, total_file_size = _check_total_file_size_changed(0, eventlog_dir)
        while (poll_num_attempts < poll_max_attempts) and (
            not _dbfs_directory_has_all_rollover_logs(eventlog_dir, run_end_time_millis)
            or _dbfs_any_file_has_zero_size(eventlog_dir)
            or file_size_changed
        ):
            if poll_num_attempts > 0:
                logger.info(
                    f"No or incomplete event log data detected - attempting again in {poll_duration_seconds} seconds"
                )
                sleep(poll_duration_seconds)

            eventlog_dir = dbx_client.list_dbfs_directory(matching_subdirectory["path"])
            file_size_changed, total_file_size = _check_total_file_size_changed(
                total_file_size, eventlog_dir
            )

            poll_num_attempts += 1

        eventlog_zip = io.BytesIO()
        eventlog_zip_file = zipfile.ZipFile(eventlog_zip, "a", zipfile.ZIP_DEFLATED)

        eventlog_files = eventlog_dir["files"]
        for eventlog_file_metadata in eventlog_files:
            filename: str = eventlog_file_metadata["path"].split("/")[-1]
            filesize: int = eventlog_file_metadata["file_size"]

            content = read_dbfs_file(eventlog_file_metadata["path"], dbx_client, filesize)

            # Databricks typically leaves the most recent rollover log uncompressed, so we may as well
            #  gzip it before upload
            if not filename.endswith(".gz"):
                content = gzip.compress(content)
                filename += ".gz"

            eventlog_zip_file.writestr(filename, content)

        eventlog_zip_file.close()
        return Response(result=eventlog_zip.getvalue())
    else:
        return Response(
            error=DatabricksError(
                message=f"No eventlog found for cluster-id: {cluster_id} & spark_context_id: {spark_context_id}"
            )
        )


def _get_eventlog(
    cluster_description: dict,
    # Databricks will deliver event logs for separate SparkApplication runs on the same cluster into different
    #  directories based on the `spark_context_id` of the Run.
    run_spark_context_id: str,
    run_end_time_millis: int,
) -> Response[bytes]:
    (log_url, filesystem, bucket, base_cluster_filepath_prefix) = _cluster_log_destination(
        cluster_description
    )
    if not filesystem:
        return Response(error=DatabricksError(message="No eventlog location found for cluster."))

    # Databricks uploads event log data every ~5 minutes to S3 -
    #   https://docs.databricks.com/clusters/configure.html#cluster-log-delivery-1
    # So we will poll this location for *up to* 5 minutes until we see all the eventlog files we are expecting
    # in the S3 bucket
    poll_duration_seconds = _event_log_poll_duration_seconds()

    if filesystem == "s3":
        return _get_eventlog_from_s3(
            cluster_id=cluster_description["cluster_id"],
            bucket=bucket,
            base_filepath=base_cluster_filepath_prefix,
            run_end_time_millis=run_end_time_millis,
            poll_duration_seconds=poll_duration_seconds,
        )
    elif filesystem == "dbfs":
        return _get_eventlog_from_dbfs(
            cluster_id=cluster_description["cluster_id"],
            spark_context_id=run_spark_context_id,
            base_filepath=base_cluster_filepath_prefix,
            run_end_time_millis=run_end_time_millis,
            poll_duration_seconds=poll_duration_seconds,
        )
    else:
        return Response(error=DatabricksError(message=f"Unknown log destination: {filesystem}"))


def _get_all_cluster_events(cluster_id: str):
    """Fetches all ClusterEvents for a given Databricks cluster, optionally within a time window.
    Pages will be followed and returned as 1 object
    """

    # Set limit to the maximum allowable value of 500 since we want to fetch all events anyway
    response = get_default_client().get_cluster_events(cluster_id, limit=500)
    responses = [response]

    next_args = response.get("next_page")
    while next_args:
        response = get_default_client().get_cluster_events(**next_args)
        responses.append(response)
        next_args = response.get("next_page")

    all_events = {
        "events": [],
        "total_count": responses[0][
            "total_count"
        ],  # total_count will be the same for all API responses
    }
    for response in responses:
        # Databricks returns cluster events from most recent --> oldest, so our paginated responses will contain
        #  older and older events as we get through them.
        all_events["events"].extend(response["events"])

    return all_events


def _update_monitored_timelines(
    running_instance_ids: Set[str],
    active_timelines_by_id: Dict[str, dict],
) -> Tuple[Dict[str, dict], List[dict]]:
    """
    Shared monitoring method for both Azure and Databricks to reduce complexity.
    Compares the current running instances (keyed by id) to the running
    instance timelines (also keyed by id). Instance timeline elements that are
    still running are updated, while the rest are returned in a "retired" list.
    """

    current_datetime = datetime.now(timezone.utc)
    for id in running_instance_ids:
        if id not in active_timelines_by_id:
            # A new instance in the "running" state has been detected, so add it
            # the dict of running instances and initialize the times.
            logger.info(f"Adding new instance timeline: {id}")
            active_timelines_by_id[id] = {
                "instance_id": id,
                "first_seen_running_time": current_datetime,
                "last_seen_running_time": current_datetime,
            }

        else:
            # If an instance was already in the list of running instances then update
            # then just update the last_seen_running_time.
            active_timelines_by_id[id]["last_seen_running_time"] = current_datetime

    # If an instance in the active timeline is no longer in the running state then
    # it should be moved over the retired timeline list.
    retired_inst_timeline_list = []
    ids_to_retire = set(active_timelines_by_id.keys()).difference(running_instance_ids)
    if ids_to_retire:
        for id in ids_to_retire:
            logger.info(f"Retiring instance: {id}")
            retired_inst_timeline_list.append(active_timelines_by_id.pop(id))

    return active_timelines_by_id, retired_inst_timeline_list


KeyType = TypeVar("KeyType")


def _deep_update(
    mapping: Dict[KeyType, Any], *updating_mappings: Dict[KeyType, Any]
) -> Dict[KeyType, Any]:
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
