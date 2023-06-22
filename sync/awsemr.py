"""
Utilities for interacting with EMR
"""

import datetime
import io
import logging
import re
from copy import deepcopy
from typing import Tuple
from urllib.parse import urlparse
from uuid import uuid4

import boto3 as boto
import orjson
from dateutil.parser import parse as dateparse

from sync import TIME_FORMAT
from sync.api import get_access_report as get_api_access_report
from sync.api.predictions import create_prediction, wait_for_prediction
from sync.api.projects import get_project
from sync.models import (
    AccessReport,
    AccessReportLine,
    AccessStatusCode,
    EMRError,
    Platform,
    ProjectError,
    Response,
)

logger = logging.getLogger(__name__)


RUN_DIR_PATTERN_TEMPLATE = r"{project_prefix}/{project_id}/(?P<timestamp>\d{{4}}-[^/]+)/{run_id}"


def get_access_report(
    log_url: str = None, cluster_id: str = None, region_name: str = None
) -> AccessReport:
    report = get_api_access_report()
    sts = boto.client("sts")
    response = sts.get_caller_identity()

    arn = response.get("Arn")
    if not arn:
        report.append(
            AccessReportLine(
                name="AWS Authentication",
                status=AccessStatusCode.RED,
                message="Failed to authenticate AWS credentials",
            )
        )
    else:
        report.append(
            AccessReportLine(
                name="AWS Authentication",
                status=AccessStatusCode.GREEN,
                message=f"Authenticated as '{arn}'",
            )
        )

    if arn and log_url:
        parsed_log_url = urlparse(log_url)

        if parsed_log_url.scheme == "s3" and arn:
            try:
                boto.client("s3").list_objects_v2(
                    Bucket=parsed_log_url.netloc,
                    Prefix=parsed_log_url.params.rstrip("/"),
                    MaxKeys=1,
                )
            except Exception:
                report.append(
                    AccessReportLine(
                        name="S3 Logging",
                        status=AccessStatusCode.RED,
                        message=f"s3:ListBucket on {parsed_log_url.geturl()} is required",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="S3 Logging",
                        status=AccessStatusCode.GREEN,
                        message=f"Can list objects at {parsed_log_url.geturl()}",
                    )
                )
        else:
            report.append(
                AccessReportLine(
                    name="Logging",
                    status=AccessStatusCode.RED,
                    message=f"scheme in {parsed_log_url.geturl()} is not supported",
                )
            )

    if arn and cluster_id:
        valid_cluster_id = True
        emr = boto.client("emr", region_name=region_name)
        try:
            emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
        except Exception as exc:
            if exc.response.get("Error", {}).get("Code") == "AccessDeniedException":
                report.append(
                    AccessReportLine(
                        name="EMR Cluster",
                        status=AccessStatusCode.RED,
                        message="emr:DescribeCluster permission is required",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="EMR Cluster",
                        status=AccessStatusCode.YELLOW,
                        message=exc.response.get("Error", {}).get("Message"),
                    )
                )
                valid_cluster_id = False
        else:
            report.append(
                AccessReportLine(
                    name="EMR Cluster",
                    status=AccessStatusCode.GREEN,
                    message="Can describe cluster",
                )
            )

        if valid_cluster_id:
            try:
                emr.list_bootstrap_actions(ClusterId=cluster_id)
            except Exception:
                report.append(
                    AccessReportLine(
                        name="EMR Bootstrap Actions",
                        status=AccessStatusCode.RED,
                        message="emr:ListBootstrapActions permission is required",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="EMR Bootstrap Actions",
                        status=AccessStatusCode.GREEN,
                        message="Can list bootstrap actions",
                    )
                )

            try:
                emr.list_instance_fleets(ClusterId=cluster_id)
            except Exception:
                report.append(
                    AccessReportLine(
                        name="EMR Instance Fleets",
                        status=AccessStatusCode.RED,
                        message="emr:ListInstanceFleets permission is required",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="EMR Instance Fleets",
                        status=AccessStatusCode.GREEN,
                        message="Can list instance fleets",
                    )
                )

            try:
                emr.list_instance_groups(ClusterId=cluster_id)
            except Exception:
                report.append(
                    AccessReportLine(
                        name="EMR Instance Fleets",
                        status=AccessStatusCode.RED,
                        message="emr:ListInstanceFleets permission is required",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="EMR Instance Fleets",
                        status=AccessStatusCode.GREEN,
                        message="Can list instance fleets",
                    )
                )

            try:
                emr.list_instances(ClusterId=cluster_id)
            except Exception:
                report.append(
                    AccessReportLine(
                        name="EMR Instances",
                        status=AccessStatusCode.RED,
                        message="emr:ListInstances permission is required",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="EMR Instances",
                        status=AccessStatusCode.GREEN,
                        message="Can list instances",
                    )
                )

            try:
                emr.list_steps(ClusterId=cluster_id)
            except Exception:
                report.append(
                    AccessReportLine(
                        name="EMR Steps",
                        status=AccessStatusCode.RED,
                        message="emr:ListSteps permission is required",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="EMR Steps", status=AccessStatusCode.GREEN, message="Can list steps"
                    )
                )

    report += get_api_access_report()

    return report


def get_project_job_flow(job_flow: dict, project_id: str) -> Response[dict]:
    """Returns a copy of the incoming job flow with project configuration.

    These tags are added:

    1. sync:run-id
    2. sync:project-id

    Additionally, if a location in S3 is configured for the project the job flow will be configured to store S3 logs there.
    The event log URL follows this pattern

      {``s3_project_url``}/{project ID}/{timestamp}/{run ID}/eventlog/

    :param job_flow: RunJobFlow request object
    :type job_flow: dict
    :param project_id: project ID
    :type project_id: str
    :return: RunJobFlow with project configuration
    :rtype: Response[dict]
    """
    result_job_flow = deepcopy(job_flow)
    project_response = get_project(project_id)
    _project = project_response.result
    if _project:
        # Add project ID tag
        run_id = str(uuid4())
        tags = {tag["Key"]: tag["Value"] for tag in result_job_flow.get("Tags", [])}
        tags["sync:project-id"] = project_id
        tags["sync:run-id"] = run_id
        result_job_flow["Tags"] = [{"Key": tag[0], "Value": tag[1]} for tag in tags.items()]

        s3_url = _project.get("s3_url")
        if s3_url:
            parsed_project_url = urlparse(f"{s3_url.strip('/')}/{project_id}")
            eventlog_props = {
                "spark.eventLog.dir": f"s3a://{parsed_project_url.netloc}/{parsed_project_url.path.strip('/')}/{datetime.datetime.utcnow().strftime(TIME_FORMAT)}/{run_id}/eventlog/",
                "spark.eventLog.enabled": "true",
            }
            for config in result_job_flow.get("Configurations", []):
                if config.get("Classification") == "spark-defaults":
                    config["Properties"] = {**config.get("Properties", {}), **eventlog_props}
                    break
            else:
                result_job_flow["Configurations"] = result_job_flow.get("Configurations", []) + [
                    {"Classification": "spark-defaults", "Properties": eventlog_props}
                ]

        return Response(result=result_job_flow)

    return project_response


def get_project_prediction(
    project_id: str, run_id: str = None, preference: str = None, region_name: str = None
) -> Response[dict]:
    """Finds the latest run in a project or one with the ID if provided (see :py:func:`~get_project_cluster_report`) and returns a prediction based on it.

    The project must be configured with an S3 URL.

    :param project_id: project ID
    :type project_id: str
    :param run_id: run ID, defaults to None
    :type run_id: str, optional
    :param preference: preferred solution defaults to None
    :type preference: str, optional
    :param region_name: AWS region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: Sync prediction
    :rtype: Response[dict]
    """
    prediction_response = create_project_prediction(project_id, run_id, preference, region_name)
    if prediction_response.error:
        return prediction_response

    return wait_for_prediction(prediction_response.result, preference)


def run_project_prediction(
    project_id: str, run_id: str = None, preference: str = None, region_name: str = None
) -> Response[str]:
    """Applies the latest prediction for a project, or one based on the run ID if provided (see :py:func:`~get_project_prediction`).
    Returns the ID of the newly created cluster.

    :param project_id: project ID
    :type project_id: str
    :param run_id: project run ID, defaults to None
    :type run_id: str, optional
    :param preference: preferred prediction solution, defaults to None
    :type preference: str, optional
    :param region_name: AWS region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: cluster ID
    :rtype: Response[str]
    """
    project_response = get_project(project_id)
    project = project_response.result
    if project:
        response = get_project_prediction(
            project_id,
            run_id,
            preference or project.get("prediction_preference", "balanced"),
            region_name,
        )
        if response.result:
            return run_job_flow(
                response.result["solutions"][
                    preference or project.get("prediction_preference", "balanced")
                ]["configuration"]
            )
    return project_response


def record_run(cluster_id: str, project_id: str, region_name: str = None) -> Response[str]:
    """Adds a report of the cluster to the project's S3 location if it has one, and
    adds to the project a prediction based on such returning the ID.

    :param cluster_id: EMR cluster ID
    :type cluster_id: str
    :param project_id: project ID
    :type project_id: str
    :param region_name: region name, defaults to None
    :type region_name: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    report_response = get_cluster_report(cluster_id, region_name)
    if report_response.error:
        return report_response

    cluster_report = report_response.result

    eventlog_url = _get_eventlog_url_from_cluster_report(cluster_report).result
    if eventlog_url:
        run_dir = _get_existing_run_dir_from_cluster_config(cluster_report, project_id).result
        if run_dir:
            if _upload_object(
                cluster_report,
                f"{run_dir}/emr-cluster-report.json",
            ).error:
                logger.warning("Failed to save configuration")

        # Start prediction
        return create_prediction(Platform.AWS_EMR, cluster_report, eventlog_url, project_id)

    return Response(error=EMRError(message="Failed to find event log"))


def create_project_prediction(
    project_id: str, run_id: str = None, region_name: str = None
) -> Response[str]:
    """Finds the latest run in a project or one with the ID if provided (see :py:func:`~get_project_cluster_report`)
    and creates a prediction based on it returning the ID.

    The project must be configured with an S3 URL.

    :param project_id: project ID
    :type project_id: str
    :param run_id: run ID, defaults to None
    :type run_id: str, optional
    :param region_name: AWS region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: Sync prediction ID
    :rtype: Response[str]
    """
    response = get_project_cluster_report(project_id, run_id, region_name)
    if response.error:
        return response

    config, eventlog_url = response.result
    return create_prediction(Platform.AWS_EMR, config, eventlog_url, project_id)


def get_project_cluster_report(  # noqa: C901
    project_id: str, run_id: str = None, region_name: str = None
) -> Response[Tuple[dict, str]]:
    """Gets the report and event log URL for the latest cluster in the project or the one identified by the `run_id` if provided.

    The project must be configured with an S3 URL.

    :param project_id: project ID
    :type project_id: str
    :param run_id: run ID, defaults to None
    :type run_id: str, optional
    :param region_name: AWS region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: a tuple containing the cluster configuration and an event log URL
    :rtype: Response[Tuple[dict, str]]
    """
    project_response = get_project(project_id)

    project = project_response.result
    if project:
        project_url = project.get("s3_url")
        if project_url:
            parsed_project_url = urlparse(f"{project_url}/{project['id']}")
            project_prefix = parsed_project_url.path.strip("/")

            s3 = boto.client("s3")
            contents = s3.list_objects_v2(
                Bucket=parsed_project_url.netloc, Prefix=project_prefix + "/"
            ).get("Contents")
            if contents:
                eventlog_pattern = re.compile(
                    rf"{project_prefix}/(?P<timestamp>\d{{4}}-[^/]+)/(?P<run_id>{run_id or '[a-zA-Z0-9-]+'})/eventlog/application_[\d_]+$"
                )

                event_logs = []
                for content in contents:
                    match = eventlog_pattern.match(content["Key"])
                    if match:
                        event_logs.append((content, match))

                event_logs.sort(key=lambda x: x[0]["LastModified"], reverse=True)
                for log_content, log_match in event_logs:
                    log_key = log_content["Key"]
                    config_key = f"{log_key[:log_key.rindex('/eventlog/')]}/emr-cluster-report.json"
                    if config_key in [content["Key"] for content in contents]:
                        config = io.BytesIO()
                        s3.download_fileobj(parsed_project_url.netloc, config_key, config)
                        return Response(
                            result=(
                                orjson.loads(config.getvalue().decode()),
                                f"s3://{parsed_project_url.netloc}/{log_key}",
                            )
                        )

                    response = _find_cluster(
                        log_match.group("run_id"),
                        created_before=log_content["LastModified"],
                        created_after=dateparse(log_match.group("timestamp")),
                        region_name=region_name,
                    )
                    cluster = response.result
                    if cluster:
                        response = get_cluster_report(cluster["Id"], region_name)
                        config = response.result
                        if config:
                            error = _upload_object(
                                config, f"s3://{parsed_project_url.netloc}/{config_key}"
                            ).error
                            if error:
                                logger.warning(f"Failed to save prediction config: {error.message}")
                            return Response(
                                result=(config, f"s3://{parsed_project_url.netloc}/{log_key}")
                            )
            else:
                return Response(
                    error=EMRError(message="No event logs with corresponding configuration found")
                )
        else:
            return Response(error=ProjectError(message="S3 URL not configured for project"))
    else:
        return project_response


def run_job_flow(job_flow: dict, project_id: str = None, region_name: str = None) -> Response[str]:
    """Creates an EMR cluster from the provided RunJobFlow request object. If a project ID
    is supplied that project's configuration is first applied. See :py:func:`~get_project_job_flow`

    If the job flow is configured to save the event log in S3 that S3 directory is created as required by Apache Spark.

    If a project with an S3 location is specified the job flow is saved at a location with the following format before the cluster is created.

      {``s3_project_url``}/{project ID}/{timestamp}/{run ID}/job-flow.json

    :param job_flow: RunJobFlow request object
    :type job_flow: dict
    :param project_id: project ID, defaults to None
    :type project_id: str, optional
    :param region_name: region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: cluster ID
    :rtype: Response[str]
    """
    if project_id:
        job_flow_response = get_project_job_flow(job_flow, project_id)
        if job_flow_response.error:
            return job_flow_response
        job_flow = job_flow_response.result
        run_id = [tag["Value"] for tag in job_flow["Tags"] if tag["Key"] == "sync:run-id"][0]

    event_log_response = create_s3_event_log_dir(job_flow)

    if project_id:
        project_response = get_project(project_id)
        if project_response.error:
            return project_response
        project = project_response.result
        if project.get("s3_url"):
            match = re.match(
                RUN_DIR_PATTERN_TEMPLATE.format(
                    project_prefix=project["s3_url"], project_id=project_id, run_id=run_id
                ),
                event_log_response.result or "",
            )
            if match:
                run_dir = match.group()
            else:
                run_dir = f"{project['s3_url']}/{project['id']}/{datetime.datetime.utcnow().strftime(TIME_FORMAT)}/{run_id}"

            error = _upload_object(
                job_flow,
                f"{run_dir}/job-flow.json",
            ).error
            if error:
                logger.warning(f"Failed to save job flow: {error.message}")

    emr = boto.client("emr", region_name=region_name)
    return Response(result=emr.run_job_flow(**job_flow)["JobFlowId"])


def run_and_record_job_flow(
    job_flow: dict, project_id: str = None, region_name: str = None
) -> Response[str]:
    """Creates an EMR cluster with the incoming RunJobFlow request object and project configuration (see :py:func:`~run_job_flow`),
    waits for the cluster to complete and records the run.

    :param job_flow: RunJobFlow request object
    :type job_flow: dict
    :param project_id: project ID
    :type project_id: str
    :param region_name: region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    job_flow_response = run_job_flow(job_flow, project_id)
    if job_flow_response.error:
        return job_flow_response

    cluster_id = job_flow_response.result

    emr = boto.client("emr", region_name=region_name)
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Waiter.ClusterTerminated
    # 30 seconds between polls, no more than 120 polls
    waiter = emr.get_waiter("cluster_terminated")
    waiter.wait(ClusterId=cluster_id, WaiterConfig={"Delay": 30, "MaxAttempts": 120})

    return record_run(cluster_id, project_id, region_name)


def create_prediction_for_cluster(cluster_id: str, region_name: str = None) -> Response[str]:
    """If the cluster terminated successfully with an event log available in S3 a prediction based
    on such is created and its ID returned.

    :param cluster_id: EMR cluster ID
    :type cluster_id: str
    :param region_name: AWS region name, defaults to None
    :type region_name: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    report_response = get_cluster_report(cluster_id, region_name)
    cluster_report = report_response.result
    if cluster_report:
        eventlog_response = _get_eventlog_url_from_cluster_report(cluster_report)
        if eventlog_response.error:
            return eventlog_response

        eventlog_http_url = eventlog_response.result
        if eventlog_http_url:
            return create_prediction(Platform.AWS_EMR, cluster_report, eventlog_http_url)

        return eventlog_response

    return cluster_report


def get_cluster_report(cluster_id: str, region_name: str = None) -> Response[dict]:
    """Get the cluster configuration required for Sync prediction

    :param cluster_id: cluster ID
    :type cluster_id: str
    :param region_name: AWS region name, defaults to AWS configuration
    :type region_name: str, optional
    :return: cluster configuration
    :rtype: Response[dict]
    """
    emr = boto.client("emr", region_name=region_name)

    cluster = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]

    # Check status
    status = cluster["Status"]
    if (
        status["State"] != "TERMINATED"
        or status["StateChangeReason"].get("Code") != "ALL_STEPS_COMPLETED"
    ):
        return Response(
            error=EMRError(
                message=f"Unexpected cluster termination state - {status['State']}: {status['StateChangeReason'].get('Code')}"
            )
        )

    cluster["BootstrapActions"] = emr.list_bootstrap_actions(ClusterId=cluster_id)[
        "BootstrapActions"
    ]
    if cluster["InstanceCollectionType"] == "INSTANCE_FLEET":
        cluster["InstanceFleets"] = emr.list_instance_fleets(ClusterId=cluster_id)["InstanceFleets"]
    else:
        cluster["InstanceGroups"] = emr.list_instance_groups(ClusterId=cluster_id)["InstanceGroups"]

    return Response(
        result={
            "Cluster": cluster,
            "Instances": emr.list_instances(ClusterId=cluster_id)["Instances"],
            "Steps": emr.list_steps(ClusterId=cluster_id)["Steps"],
            "Region": region_name or emr.meta.region_name,
        }
    )


def create_s3_event_log_dir(job_flow: dict) -> Response[str]:
    """Creates the event log "directory" in S3 if the incoming RunJobFlow request object is configured with one.

    :param job_flow: RunJobFlow request object
    :type job_flow: dict
    :return: S3 event log directory URL
    :rtype: Response[str]
    """
    for config in job_flow["Configurations"]:
        if config["Classification"] == "spark-defaults":
            eventlog_dir = config["Properties"].get("spark.eventLog.dir")
            if (
                eventlog_dir
                and config["Properties"].get("spark.eventLog.enabled", "false").lower() == "true"
            ):
                parsed_eventlog_dir = urlparse(eventlog_dir)
                if parsed_eventlog_dir.scheme == "s3a":
                    try:
                        s3 = boto.client("s3")
                        s3.put_object(
                            Bucket=parsed_eventlog_dir.netloc,
                            Key=parsed_eventlog_dir.path.lstrip("/"),
                        )
                        return Response(
                            result=f"s3://{parsed_eventlog_dir.netloc}/{parsed_eventlog_dir.path.lstrip('/')}"
                        )
                    except Exception as exc:
                        return Response(error=EMRError(message=str(exc)))

    return Response(error=EMRError(message="No S3 event log dir configured"))


def _get_eventlog_url_from_cluster_report(cluster_config: dict) -> Response[str]:
    """Returns an S3 URL to the event log for the cluster if one - and only one - exists."""
    eventlog_dir = None
    for config in cluster_config["Cluster"]["Configurations"]:
        if config["Classification"] == "spark-defaults":
            eventlog_dir = config["Properties"].get("spark.eventLog.dir")
            break

    if not eventlog_dir:
        return Response(
            error=EMRError(message="Failed to find event log directory in cluster configuration")
        )

    parsed_eventlog_dir = urlparse(eventlog_dir)
    eventlog_pattern = re.compile(rf"{parsed_eventlog_dir.path.lstrip('/')}application_[\d_]+$")
    s3 = boto.client("s3")
    s3_objects = s3.list_objects_v2(
        Bucket=parsed_eventlog_dir.netloc, Prefix=parsed_eventlog_dir.path.lstrip("/")
    )
    eventlog_keys = [c["Key"] for c in s3_objects["Contents"] if eventlog_pattern.match(c["Key"])]

    if not eventlog_keys:
        return Response(error=EMRError(message="No event log found"))
    if len(eventlog_keys) > 1:
        return Response(error=EMRError(message="More than 1 event log found"))

    return Response(result=f"s3://{parsed_eventlog_dir.netloc}/{eventlog_keys[0]}")


def _get_existing_run_dir_from_cluster_config(
    cluster_config: dict, project_id: str
) -> Response[str]:
    for tag in cluster_config["Cluster"].get("Tags", []):
        if tag["Key"] == "sync:run-id":
            run_id = tag["Value"]
            break
    else:
        return Response(error=EMRError(message="Failed to find run ID in cluster configuration"))
    cluster_start_time = cluster_config["Cluster"]["Status"]["Timeline"]["CreationDateTime"]

    return _get_existing_run_dir(project_id, run_id, cluster_start_time)


def _get_existing_run_dir(
    project_id: str, run_id: str, cluster_start_time: datetime.datetime = None
) -> Response[str]:
    project_response = get_project(project_id)
    project = project_response.result
    if project:
        s3_url = project.get("s3_url")
        if s3_url:
            parsed_s3_url = urlparse(s3_url)
            s3 = boto.client("s3")
            run_dir_pattern = re.compile(
                RUN_DIR_PATTERN_TEMPLATE.format(
                    project_prefix=parsed_s3_url.path.strip("/"),
                    project_id=project_id,
                    run_id=run_id,
                )
            )
            start_after = (cluster_start_time - datetime.timedelta(hours=1)).isoformat()
            list_response = s3.list_objects_v2(
                Bucket=parsed_s3_url.netloc,
                Prefix=f"{parsed_s3_url.path.strip('/')}/{project_id}/",
                StartAfter=start_after,
            )
            contents = list_response.get("Contents")
            continuation_token = list_response.get("NextContinuationtoken")
            pages_left = 5
            while pages_left and contents:
                for content in contents:
                    match = run_dir_pattern.match(content["Key"])
                    if match:
                        return Response(
                            result=f"s3://{parsed_s3_url.netloc}/{match.group().rstrip('/')}"
                        )
                if continuation_token:
                    list_response = s3.list_objects_v2(
                        Bucket=parsed_s3_url.netloc,
                        Prefix=f"{parsed_s3_url.path.strip('/')}/{project_id}/",
                        StartAfter=start_after,
                        ContinuationToken=continuation_token,
                    )
                    contents = list_response.get("Contents")
                    continuation_token = list_response.get("NextContinuationtoken")
                else:
                    contents = []
                pages_left -= 1
            return Response(error=EMRError(message="Existing run directory not found"))
        return Response(error=EMRError(message="No S3 URL configured for project"))
    return project_response


def _find_cluster(
    run_id: str,
    created_before: datetime.datetime,
    created_after: datetime.datetime = None,
    region_name: str = None,
) -> Response[dict]:
    """Returns an AWS DescribeCluster response object if an EMR cluster tagged with the run ID can be found within the date range."""
    created_after = created_after or created_before - datetime.timedelta(days=3)
    emr = boto.client("emr", region_name=region_name)
    response = emr.list_clusters(
        CreatedBefore=created_before,
        CreatedAfter=created_after,
        ClusterStates=["TERMINATED"],
    )
    clusters = response.get("Clusters")
    marker = response.get("Marker")

    pages_left = 5
    while clusters:
        for cluster in clusters:
            if cluster["Status"]["StateChangeReason"].get("Code") == "ALL_STEPS_COMPLETED":
                cluster_detail = emr.describe_cluster(ClusterId=cluster["Id"])["Cluster"]
                if [
                    tag
                    for tag in cluster_detail["Tags"]
                    if tag["Key"] == "sync:run-id" and tag["Value"] == run_id
                ]:
                    return Response(result=cluster_detail)

        pages_left -= 1
        if pages_left and marker:
            response = emr.list_clusters(
                CreatedBefore=created_before,
                CreatedAfter=created_after,
                ClusterStates=["TERMINATED"],
                Marker=marker,
            )
            clusters = response["Clusters"]
            marker = response.get("Marker")
        elif pages_left:
            return Response(
                error=EMRError(message="No matching cluster in the specified time period")
            )
        else:
            return Response(error=EMRError(message="Matching EMR cluster not found"))

    return Response(error=EMRError(message="Failed to find EMR cluster"))


def _upload_object(obj: dict, s3_url: str) -> Response[str]:
    parsed_url = urlparse(s3_url)
    obj_key = parsed_url.path.lstrip("/")

    try:
        s3 = boto.client("s3")
        s3.upload_fileobj(
            io.BytesIO(
                orjson.dumps(
                    obj,
                    option=orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_NAIVE_UTC,
                )
            ),
            parsed_url.netloc,
            obj_key,
        )

        return Response(result=f"s3://{parsed_url.netloc}/{obj_key}")
    except Exception as exc:
        return Response(error=EMRError(message=f"Failed to save object: {exc}"))
