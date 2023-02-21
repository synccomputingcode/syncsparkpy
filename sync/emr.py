"""
Utilities for interacting with EMR
"""

import datetime
import io
import logging
import re
from copy import deepcopy
from urllib.parse import urlparse
from uuid import uuid4

import boto3 as boto
import orjson
from dateutil.parser import parse as dateparse

from .api.predictions import generate_prediction, generate_presigned_url, initiate_prediction
from .api.projects import get_project
from .client import get_default_client
from .models import Error, Response

logger = logging.getLogger(__name__)


def get_project_job_flow(job_flow: dict, project_id: str) -> Response[dict]:
    result_job_flow = deepcopy(job_flow)
    project_response = get_project(project_id)
    if _project := project_response.result:
        # Add project ID tag
        run_id = str(uuid4())
        tags = {tag["Key"]: tag["Value"] for tag in result_job_flow.get("Tags", [])}
        tags["sync:project-id"] = project_id
        tags["sync:run-id"] = run_id
        result_job_flow["Tags"] = [{"Key": tag[0], "Value": tag[1]} for tag in tags.items()]

        if s3_url := _project.get("s3_url"):
            parsed_project_url = urlparse(f"{s3_url.strip('/')}/{project_id}")
            eventlog_props = {
                "spark.eventLog.dir": f"s3a://{parsed_project_url.netloc}/{parsed_project_url.path.strip('/')}/{datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}/{run_id}/",
                "spark.eventLog.enabled": "true",
            }
            for config in result_job_flow.get("Configurations", []):
                if config.get("Classification") == "spark-defaults":
                    config["Properties"] = {**config.get("Properties", {}), **eventlog_props}
                    break
            else:
                result_job_flow["Configurations"] = result_job_flow.get("Configurations", []) + [
                    {"Classifiaction": "spark-defaults", "Properties": eventlog_props}
                ]

        return Response(result=result_job_flow)

    return project_response


def run_job_flow(job_flow: dict, project_id: str = None) -> Response[str]:
    """Creates an EMR job flow

    Args:
        job_flow (dict): job flow spec

    Returns:
        str: job flow ID
    """
    if project_id:
        job_flow_response = get_project_job_flow(job_flow, project_id)
        if job_flow_response.error:
            return job_flow_response
        job_flow = job_flow_response.result

    # Create event log dir if configured - Spark requires a directory
    for config in job_flow["Configurations"]:
        if config["Classification"] == "spark-defaults":
            if (eventlog_dir := config["Properties"].get("spark.eventLog.dir")) and config[
                "Properties"
            ].get("spark.eventLog.enabled", "false").lower() == "true":
                parsed_eventlog_dir = urlparse(eventlog_dir)
                if parsed_eventlog_dir.scheme == "s3a":
                    s3 = boto.client("s3")
                    s3.put_object(
                        Bucket=parsed_eventlog_dir.netloc, Key=parsed_eventlog_dir.path.lstrip("/")
                    )
                    logger.info(f"Created event log dir at {eventlog_dir}")

                    if project_id:
                        if error := upload_object(
                            job_flow,
                            f"s3://{parsed_eventlog_dir.netloc}/{parsed_eventlog_dir.path.strip('/')}/job-flow.json",
                        ).error:
                            logger.warning(f"Failed to save job flow: {error.message}")
                    break

    emr = boto.client("emr")
    return Response(result=emr.run_job_flow(**job_flow)["JobFlowId"])


def run_and_wait_for_job_flow(job_flow: dict, project_id: str = None) -> Response[str]:
    job_flow_response = run_job_flow(job_flow, project_id)
    if job_flow_response.error:
        return job_flow_response

    cluster_id = job_flow_response.result

    emr = boto.client("emr")
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Waiter.ClusterTerminated
    # 30 seconds between polls, no more than 120 polls
    waiter = emr.get_waiter("cluster_terminated")
    waiter.wait(ClusterId=cluster_id, WaiterConfig={"Delay": 30, "MaxAttempts": 120})

    return record_run(cluster_id, project_id)


def record_run(cluster_id: str, project_id: str = None) -> Response[str]:
    config_response = get_cluster_config(cluster_id)
    if config_response.error:
        return config_response

    config = config_response.result

    # Check state
    status = config["Cluster"]["Status"]
    if (
        status["State"] != "TERMINATED"
        or status["StateChangeReason"].get("Code") != "ALL_STEPS_COMPLETED"
    ):
        return Response(
            error=Error(
                code="EMR Error",
                message=f"Unexpected cluster termination state - {status['State']}: {status['StateChangeReason'].get('Code')}",
            )
        )

    # Save configuration
    if eventlog_url := get_eventlog_url_from_cluster_config(config).result:
        if project_id:
            parsed_eventlog_url = urlparse(eventlog_url)
            if upload_object(
                config,
                f"s3://{parsed_eventlog_url.netloc}/{parsed_eventlog_url.path[:parsed_eventlog_url.path.rindex('/')].strip('/')}/config.json",
            ).error:
                logger.warning("Failed to save configuration")

    # Start prediction
    return initiate_prediction(config, eventlog_url)


def upload_object(obj: dict, s3_url: str) -> Response[str]:
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
        return Response(error=Error(code="EMR Error", message=f"Failed to save object: {exc}"))


def initiate_prediction_with_cluster_id(cluster_id: str) -> Response[str]:
    cluster_response = get_cluster_config(cluster_id)
    if cluster_config := cluster_response.result:
        eventlog_response = get_eventlog_url_from_cluster_config(cluster_config)
        if eventlog_response.error:
            return eventlog_response

        eventlog_http_url_response = generate_presigned_url(eventlog_response.result)

        if eventlog_http_url := eventlog_http_url_response.result:
            return initiate_prediction(cluster_config, eventlog_http_url)
        return eventlog_http_url_response
    return cluster_response


def get_eventlog_url_from_cluster_config(cluster_config: dict) -> Response[str]:
    eventlog_dir = None
    for config in cluster_config["Cluster"]["Configurations"]:
        if config["Classification"] == "spark-defaults":
            eventlog_dir = config["Properties"].get("spark.eventLog.dir")
            break

    if not eventlog_dir:
        return Response(
            error=Error(
                code="Prediction error",
                message="Failed to find event log directory in cluster configuration",
            )
        )

    parsed_eventlog_dir = urlparse(eventlog_dir)
    eventlog_pattern = re.compile(rf"{parsed_eventlog_dir.path.lstrip('/')}application_[\d_]+$")
    s3 = boto.client("s3")
    s3_objects = s3.list_objects_v2(
        Bucket=parsed_eventlog_dir.netloc, Prefix=parsed_eventlog_dir.path.lstrip("/")
    )
    eventlog_keys = [c["Key"] for c in s3_objects["Contents"] if eventlog_pattern.match(c["Key"])]

    if not eventlog_keys:
        return Response(error=Error(code="Prediction error", message="No event log found"))
    if len(eventlog_keys) > 1:
        return Response(error=Error(code="Prediction error", message="More than 1 event log found"))

    return Response(result=f"s3://{parsed_eventlog_dir.netloc}/{eventlog_keys[0]}")


def get_cluster_config(cluster_id: str) -> Response[dict]:
    emr = boto.client("emr")

    cluster = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
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
            "Region": emr.meta.region_name,
        }
    )


def find_cluster(
    run_id: str, created_before: datetime.datetime, created_after: datetime.datetime = None
) -> Response[dict]:
    created_after = created_after or created_before - datetime.timedelta(days=3)
    emr = boto.client("emr")
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
                error=Error(
                    code="EMR Error", message="No matching cluster in the specified time period"
                )
            )
        else:
            return Response(error=Error(code="EMR Error", message="Matching EMR cluster not found"))

    return Response(error=Error(code="EMR Error", message="Failed to find EMR cluster"))


def get_latest_config(  # noqa: C901
    project_id: str, run_id: str = None
) -> Response[tuple[dict, str]]:
    sync = get_default_client()
    response = sync.get_project(project_id)

    if project := response.get("result"):
        if project_url := project.get("s3_url"):
            parsed_project_url = urlparse(f"{project_url}/{project['id']}")
            project_prefix = parsed_project_url.path.strip("/")

            s3 = boto.client("s3")
            if contents := s3.list_objects_v2(
                Bucket=parsed_project_url.netloc, Prefix=project_prefix + "/"
            ).get("Contents"):
                eventlog_pattern = re.compile(
                    rf"{project_prefix}/(?P<timestamp>\d{{4}}-[^/]+)/(?P<run_id>{run_id or '[a-zA-Z0-9-]+'})/application_[\d_]+$"
                )

                event_logs = []
                for content in contents:
                    match = eventlog_pattern.match(content["Key"])
                    if match:
                        event_logs.append((content, match))

                event_logs.sort(key=lambda x: x[0]["LastModified"], reverse=True)
                for log_content, log_match in event_logs:
                    log_key = log_content["Key"]
                    config_key = f"{log_key[:log_key.rindex('/')]}/config.json"
                    if config_key in [content["Key"] for content in contents]:
                        config = io.BytesIO()
                        s3.download_fileobj(parsed_project_url.netloc, config_key, config)
                        return Response(
                            result=(
                                orjson.loads(config.getvalue().decode()),
                                f"s3://{parsed_project_url.netloc}/{log_key}",
                            )
                        )

                    response = find_cluster(
                        log_match.group("run_id"),
                        created_before=log_content["LastModified"],
                        created_after=dateparse(log_match.group("timestamp")),
                    )
                    if cluster := response.result:
                        response = get_cluster_config(cluster["Id"])
                        if config := response.result:
                            if error := upload_object(
                                config, f"s3://{parsed_project_url.netloc}/{config_key}"
                            ).error:
                                logger.warning(f"Failed to save prediction config: {error.message}")
                            return Response(
                                result=(config, f"s3://{parsed_project_url.netloc}/{log_key}")
                            )
            else:
                return Response(
                    error=Error("EMR Error", "No event logs with corresponding configuration found")
                )
        else:
            return Response(error=Error("Project Error", "S3 URL not configured for project"))
    else:
        return Response(
            error=Error(
                "Project Error", f"{response['error']['code']}:{response['error']['message']}"
            )
        )


def generate_latest_prediction(
    project_id: str, run_id: str = None, preference: str = None
) -> Response[dict]:
    response = get_latest_config(project_id, run_id)
    if response.result:
        config, eventlog_url = response.result

        return generate_prediction(config, eventlog_url, preference)
    return response


def apply_latest_prediction(project_id: str) -> Response[str]:
    response = get_default_client().get_project(project_id)
    if project := response.get("result"):
        response = generate_latest_prediction(project_id, project.get("prediction_preference"))
        if response.result:
            return run_job_flow(
                response.result["solutions"][project.get("prediction_preference", "balanced")][
                    "configuration"
                ]
            )
