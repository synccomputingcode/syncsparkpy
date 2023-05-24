"""
Utilities for interacting with Databricks
"""
import datetime
import io
import logging
import time
import zipfile
from collections.abc import Collection
from pathlib import Path
from time import sleep
from typing import Any, TypeVar
from urllib.parse import urlparse

import boto3 as boto
from orjson import orjson

from sync.api.predictions import create_prediction_with_eventlog_bytes, get_prediction
from sync.api.projects import get_project
from sync.clients.databricks import get_default_client
from sync.config import CONFIG, DB_CONFIG
from sync.models import (
    DatabricksAPIError,
    DatabricksClusterReport,
    DatabricksError,
    Platform,
    Response,
)

logger = logging.getLogger(__name__)


def create_prediction(
    plan_type: str,
    compute_type: str,
    cluster: dict,
    cluster_events: dict,
    instances: dict,
    eventlog: bytes,
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
    :param instances: All EC2 Instances that were a part of the cluster. Expects a data format as is returned by
        boto3's EC2.describe_instances API - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/client/describe_instances.html
        Instances should be narrowed to just those instances relevant to the Databricks Run. This can be done by passing
        a `tag:ClusterId` filter to the describe_instances call like -
        Filters=[{"Name": "tag:ClusterId", "Values": ["my-dbx-clusterid"]}]
        If there are multiple pages of instances, all pages should be accumulated into 1 dictionary and passed to this
        function
    :param eventlog: encoded event log zip
    :type eventlog: bytes
    :param project_id: Sync project ID, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    return create_prediction_with_eventlog_bytes(
        Platform.AWS_DATABRICKS,
        {
            "plan_type": plan_type,
            "compute_type": compute_type,
            "cluster": cluster,
            "cluster_events": cluster_events,
            "instances": instances,
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
    exclude_tasks: Collection[str] | None = None,
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

    tasks = [
        task for task in run["tasks"] if not exclude_tasks or task["task_key"] not in exclude_tasks
    ]

    if any(task["state"].get("result_state") != "SUCCESS" for task in tasks):
        return Response(error=DatabricksError(message="Tasks did not complete successfully"))

    cluster_id_response = _get_run_cluster_id(tasks)
    if cluster_id := cluster_id_response.result:
        # Making these calls prior to fetching the event log allows Databricks a little extra time to finish
        #  uploading all the event log data before we start checking for it
        cluster_report_response = _get_cluster_report(
            cluster_id, plan_type, compute_type, allow_incomplete_cluster_report
        )
        if cluster_report := cluster_report_response.result:

            cluster = cluster_report.cluster
            eventlog_response = _get_eventlog(cluster, run.get("end_time"))

            if eventlog := eventlog_response.result:
                return create_prediction(
                    plan_type=cluster_report.plan_type.value,
                    compute_type=cluster_report.compute_type.value,
                    cluster=cluster,
                    cluster_events=cluster_report.cluster_events,
                    instances=cluster_report.instances,
                    eventlog=eventlog,
                    project_id=project_id,
                )

            return eventlog_response
        return cluster_report_response
    return cluster_id_response


def get_cluster_report(
    run_id: str,
    plan_type: str,
    compute_type: str,
    allow_incomplete: bool = False,
    exclude_tasks: Collection[str] | None = None,
) -> Response[DatabricksClusterReport]:
    """Fetches the cluster information required to create a Sync prediction

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: Databricks Pricing Plan, e.g. "Standard"
    :type plan_type: str
    :param compute_type: Cluster compute type, e.g. "Jobs Compute"
    :type compute_type: str
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

    tasks = [
        task for task in run["tasks"] if not exclude_tasks or task["task_key"] not in exclude_tasks
    ]

    if any(task["state"].get("result_state") != "SUCCESS" for task in tasks):
        return Response(error=DatabricksError(message="Tasks did not complete successfully"))

    cluster_id_response = _get_run_cluster_id(tasks)
    if cluster_id := cluster_id_response.result:
        return _get_cluster_report(cluster_id, plan_type, compute_type, allow_incomplete)

    return cluster_id_response


def _get_cluster_report(
    cluster_id: str, plan_type: str, compute_type: str, allow_incomplete: bool
) -> Response[DatabricksClusterReport]:
    # Cluster `terminated_time` can be a few seconds after the start of the next task in which
    # this may be executing.
    cluster_response = _wait_for_cluster_termination(cluster_id, timeout_seconds=60, poll_seconds=5)
    if cluster_response.error:
        return cluster_response

    cluster = cluster_response.result

    # Making these calls prior to fetching the event log allows Databricks a little extra time to finish
    #  uploading all the event log data before we start checking for it
    cluster_events = _get_all_cluster_events(cluster_id)
    aws_region_name = DB_CONFIG.aws_region_name
    ec2 = boto.client("ec2", region_name=aws_region_name)

    instances = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Vendor", "Values": ["Databricks"]},
            {"Name": "tag:ClusterId", "Values": [cluster_id]},
            # {'Name': 'tag:JobId', 'Values': []}
        ]
    )
    if not instances["Reservations"]:
        no_instances_message = (
            f"Unable to find any active or recently terminated instances for cluster `{cluster_id}` in `{aws_region_name}`. "
            + "Please refer to the following documentation for options on how to address this - "
            + "https://synccomputingcode.github.io/syncsparkpy/reference/awsdatabricks.html"
        )
        if allow_incomplete:
            logger.warning(no_instances_message)
        else:
            return Response(error=DatabricksError(message=no_instances_message))

    return Response(
        result=DatabricksClusterReport(
            plan_type=plan_type,
            compute_type=compute_type,
            cluster=cluster,
            cluster_events=cluster_events,
            instances=instances,
        )
    )


def record_run(
    run_id: str,
    plan_type: str,
    compute_type: str,
    project_id: str,
    allow_incomplete_cluster_report: bool = False,
    exclude_tasks: Collection[str] | None = None,
) -> Response[str]:
    """See :py:func:`~create_prediction_for_run`

    :param run_id: Databricks run ID
    :type run_id: str
    :param plan_type: either "Standard", "Premium" or "Enterprise"
    :type plan_type: str
    :param compute_type: e.g. "Jobs Compute"
    :type compute_type: str
    :param project_id: Sync project ID, defaults to None
    :type project_id: str
    :param allow_incomplete_cluster_report: Whether creating a prediction with incomplete cluster report data should be allowable
    :type allow_incomplete_cluster_report: bool, optional, defaults to False
    :param exclude_tasks: Keys of tasks (task names) to exclude
    :type exclude_tasks: Collection[str], optional, defaults to None
    :return: prediction ID
    :rtype: Response[str]
    """
    return create_prediction_for_run(
        run_id, plan_type, compute_type, project_id, allow_incomplete_cluster_report, exclude_tasks
    )


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

                if cluster_key := tasks[0].get("job_cluster_key"):
                    job_settings["job_clusters"] = [
                                                       j
                                                       for j in job_settings["job_clusters"]
                                                       if j.get("job_cluster_key") != cluster_key
                                                   ] + [{"job_cluster_key": cluster_key,
                                                         "new_cluster": prediction_cluster}]
                else:
                    tasks[0]["new_cluster"] = prediction_cluster
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
                if cluster_key := tasks[0].get("job_cluster_key"):
                    job_settings["job_clusters"] = [
                                                       j
                                                       for j in job_settings["job_clusters"]
                                                       if j.get("job_cluster_key") != cluster_key
                                                   ] + [
                                                       {"job_cluster_key": cluster_key, "new_cluster": project_cluster}]
                else:
                    tasks[0]["new_cluster"] = project_cluster

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
    """Terminate Databricks cluster and wait to return final state.

    :param cluster_id: Databricks cluster ID
    :type cluster_id: str
    :return: Databricks cluster object with state: "TERMINATED"
    :rtype: Response[str]
    """
    cluster = get_default_client().get_cluster(cluster_id)
    if "error_code" not in cluster:
        state = cluster.get("state")
        match state:
            case "TERMINATED":
                return Response(result=cluster)
            case "TERMINATING":
                return _wait_for_cluster_termination(cluster_id)
            case "PENDING" | "RUNNING" | "RESTARTING" | "RESIZING":
                get_default_client().delete_cluster(cluster_id)
                return _wait_for_cluster_termination(cluster_id)
            case _:
                return Response(error=DatabricksError(message=f"Unexpected cluster state: {state}"))

    return Response(error=DatabricksAPIError(**cluster))


def _wait_for_cluster_termination(
    cluster_id: str, timeout_seconds=300, poll_seconds=10
) -> Response[str]:
    start_seconds = time.time()
    cluster = get_default_client().get_cluster(cluster_id)
    while "error_code" not in cluster:
        state = cluster.get("state")
        match state:
            case "TERMINATED":
                return Response(result=cluster)
            case "TERMINATING":
                sleep(poll_seconds)
            case _:
                return Response(error=DatabricksError(message=f"Unexpected cluster state: {state}"))

        if time.time() - start_seconds > timeout_seconds:
            return Response(
                error=DatabricksError(
                    message=f"Cluster failed to terminate after waiting {timeout_seconds} seconds"
                )
            )

        cluster = get_default_client().get_cluster(cluster_id)

    return Response(error=DatabricksAPIError(**cluster))


def monitor_cluster(cluster_id: str) -> None:
    s3 = boto.client("s3")
    aws_region_name = DB_CONFIG.aws_region_name
    ec2 = boto.client("ec2", region_name=aws_region_name)

    cluster = get_default_client().get_cluster(cluster_id)
    logger.warn(cluster)

    log_url = cluster.get("cluster_log_conf", {}).get("s3", {}).get("destination")

    parsed_log_url = urlparse(log_url)
    stripped_path = parsed_log_url.path.strip("/")

    # TODO - we may want to grab the Driver instance first by looking for an instance matching the $DB_DRIVER_IP.
    #  Given the tags on the instance, we can then look for further worker instances.

    # If the event log destination is just a *bucket* without any sub-path, then we don't want to include
    #  a leading `/` in our Prefix (which will make it so that we never actually find the event log), so
    #  we make sure to re-strip our final Prefix
    file_key = f"{stripped_path}/{cluster_id}/sync_data/cluster_instances.json".strip("/")
    previous_instances = None

    while True:
        instances = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Vendor", "Values": ["Databricks"]},
                {"Name": "tag:ClusterId", "Values": [cluster_id]},
                # {'Name': 'tag:JobId', 'Values': []}
            ]
        )

        if previous_instances:
            logger.info("Merging instances....")
            # TODO
            new_instances = [res for res in instances['Reservations']]
            new_instance_id_to_reservation = dict(zip(
                [res['Instances'][0]['InstanceId'] for res in new_instances],
                new_instances
            ))

            old_instances = [res for res in previous_instances['Reservations']]
            old_instance_id_to_reservation = dict(zip(
                [res['Instances'][0]['InstanceId'] for res in old_instances],
                old_instances
            ))

            old_instance_ids = set(old_instance_id_to_reservation.keys())
            new_instance_ids = set(new_instance_id_to_reservation.keys())

            # If we have the exact same set of instances, prefer the new set...
            if old_instance_ids == new_instance_ids:
                instances = {
                    'Reservations': new_instances
                }
            else:
                # Otherwise, update old references and include any new instances in the list
                newly_added_instance_ids = new_instance_ids.difference(old_instance_ids)
                updated_instance_ids = newly_added_instance_ids.intersection(old_instance_ids)
                removed_instance_ids = old_instance_ids.difference(updated_instance_ids)

                removed_instances = [old_instance_id_to_reservation[id] for id in removed_instance_ids]
                updated_instances = [new_instance_id_to_reservation[id] for id in updated_instance_ids]
                new_instances = [new_instance_id_to_reservation[id] for id in newly_added_instance_ids]

                instances = {
                    'Reservations': [*removed_instances, *updated_instances, *new_instances]
                }

        s3.put_object(Bucket=parsed_log_url.netloc, Key=file_key, Body=orjson.dumps(instances))

        previous_instances = instances

        sleep(30)


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


def _get_run_cluster_id(tasks: list[dict]) -> Response[str]:
    cluster_ids = {task["cluster_instance"]["cluster_id"] for task in tasks}
    match len(cluster_ids):
        case 1:
            return Response(result=cluster_ids.pop())
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


def _s3_contents_have_all_rollover_logs(contents: list[dict], run_end_time_seconds: float):
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


def event_log_poll_duration_seconds():
    """Convenience function to aid testing"""
    return 15


def _get_eventlog(cluster_description: dict, run_end_time_millis: int) -> Response[bytes]:
    log_url = cluster_description.get("cluster_log_conf", {}).get("s3", {}).get("destination")
    if log_url:
        parsed_log_url = urlparse(log_url)

        cluster_id = cluster_description["cluster_id"]
        s3 = boto.client("s3")

        stripped_path = parsed_log_url.path.strip("/")
        # If the event log destination is just a *bucket* without any sub-path, then we don't want to include
        #  a leading `/` in our Prefix (which will make it so that we never actually find the event log), so
        #  we make sure to re-strip our final Prefix
        prefix = f"{stripped_path}/{cluster_id}/eventlog/{cluster_id}".strip("/")

        logger.info(f"Looking for eventlogs at location: {prefix}")

        # Databricks uploads event log data every ~5 minutes to S3 -
        #   https://docs.databricks.com/clusters/configure.html#cluster-log-delivery-1
        # So we will poll this location for *up to* 5 minutes until we see all the eventlog files we are expecting
        # in the S3 bucket
        poll_duration_seconds = event_log_poll_duration_seconds()
        num_attempts = 0
        max_attempts = 20  # 5 minutes / 15 seconds = 20 attempts
        contents = None
        run_end_time_seconds = run_end_time_millis / 1000
        while (
            not _s3_contents_have_all_rollover_logs(contents, run_end_time_seconds)
            and num_attempts < max_attempts
        ):
            if num_attempts > 0:
                logger.info(
                    f"No or incomplete event log data detected - attempting again in {poll_duration_seconds} seconds"
                )
                sleep(poll_duration_seconds)

            contents = s3.list_objects_v2(
                Bucket=parsed_log_url.netloc,
                Prefix=prefix,
            ).get("Contents")
            num_attempts += 1

        if contents:
            eventlog_zip = io.BytesIO()
            eventlog_zip_file = zipfile.ZipFile(eventlog_zip, "a", zipfile.ZIP_DEFLATED)

            for content in contents:
                obj = s3.get_object(Bucket=parsed_log_url.netloc, Key=content["Key"])
                eventlog_zip_file.writestr(content["Key"].split("/")[-1], obj["Body"].read())

            eventlog_zip_file.close()

            return Response(result=eventlog_zip.getvalue())

        return Response(
            error=DatabricksError(
                message=f"No eventlog found at location - {log_url} - after {num_attempts * poll_duration_seconds} seconds"
            )
        )


def _get_all_cluster_events(cluster_id: str):
    """Fetches all ClusterEvents for a given Databricks cluster, optionally within a time window.
    Pages will be followed and returned as 1 object
    """

    # Set limit to the maximum allowable value of 500 since we want to fetch all events anyway
    response = get_default_client().get_cluster_events(cluster_id, limit=500)
    responses = [response]

    while next_args := response.get("next_page"):
        response = get_default_client().get_cluster_events(cluster_id, **next_args)
        responses.append(response)

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
