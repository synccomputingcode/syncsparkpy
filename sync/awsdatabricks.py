import json
import logging
from pathlib import Path
from time import sleep
from typing import List, Tuple
from urllib.parse import urlparse

import boto3 as boto
import botocore
from botocore.exceptions import ClientError

import sync._databricks
from sync._databricks import (
    _cluster_log_destination,
    _get_all_cluster_events,
    _get_cluster_instances_from_dbfs,
    _update_monitored_timelines,
    _wait_for_cluster_termination,
    apply_project_recommendation,
    create_and_record_run,
    create_and_wait_for_run,
    create_cluster,
    create_run,
    create_submission_for_run,
    get_cluster,
    get_cluster_report,
    get_project_cluster,
    get_project_cluster_settings,
    get_project_job,
    get_recommendation_job,
    handle_successful_job_run,
    record_run,
    run_and_record_job,
    run_and_record_job_object,
    run_and_record_project_job,
    run_job_object,
    terminate_cluster,
    wait_for_and_record_run,
    wait_for_final_run_status,
    wait_for_run_and_cluster,
)
from sync.api import get_access_report as get_api_access_report
from sync.clients.databricks import get_default_client
from sync.config import DB_CONFIG
from sync.models import (
    AccessReport,
    AccessReportLine,
    AccessStatusCode,
    AWSDatabricksClusterReport,
    DatabricksError,
    Response,
)
from sync.utils.dbfs import format_dbfs_filepath, write_dbfs_file
from sync.utils.json import DefaultDateTimeEncoder

__all__ = [
    "get_access_report",
    "run_and_record_job",
    "create_submission_for_run",
    "get_cluster_report",
    "monitor_cluster",
    "create_cluster",
    "get_cluster",
    "handle_successful_job_run",
    "record_run",
    "get_project_job",
    "get_project_cluster",
    "get_project_cluster_settings",
    "get_recommendation_job",
    "run_job_object",
    "create_run",
    "run_and_record_project_job",
    "run_and_record_job_object",
    "create_and_record_run",
    "wait_for_and_record_run",
    "create_and_wait_for_run",
    "wait_for_final_run_status",
    "wait_for_run_and_cluster",
    "terminate_cluster",
    "apply_project_recommendation",
]


logger = logging.getLogger(__name__)


def get_access_report(log_url: str = None) -> AccessReport:
    """Reports access to Databricks, AWS and Sync required for integrating jobs with Sync.
    Access is partially determined by the configuration of this library and boto3.

    :param log_url: location of event logs, defaults to None
    :type log_url: str, optional
    :return: access report
    :rtype: AccessReport
    """
    report = get_api_access_report()
    dbx_client = get_default_client()

    response = dbx_client.get_current_user()
    user_name = response.get("userName")
    if user_name:
        report.append(
            AccessReportLine(
                name="Databricks Authentication",
                status=AccessStatusCode.GREEN,
                message=f"Authenticated as '{user_name}'",
            )
        )
    else:
        report.append(
            AccessReportLine(
                name="Databricks Authentication",
                status=AccessStatusCode.RED,
                message=f"{response.get('error_code')}: {response.get('message')}",
            )
        )

    response = boto.client("sts").get_caller_identity()
    arn = response.get("Arn")
    if arn:
        report.append(
            AccessReportLine(
                name="AWS Authentication",
                status=AccessStatusCode.GREEN,
                message=f"Authenticated as '{arn}'",
            )
        )

        ec2 = boto.client("ec2", region_name=DB_CONFIG.aws_region_name)
        report.add_boto_method_call(ec2.describe_instances, AccessStatusCode.YELLOW, DryRun=True)
        report.add_boto_method_call(ec2.describe_volumes, AccessStatusCode.YELLOW, DryRun=True)
    else:
        report.append(
            AccessReportLine(
                name="AWS Authentication",
                status=AccessStatusCode.RED,
                message="Failed to authenticate AWS credentials",
            )
        )

    if log_url:
        parsed_log_url = urlparse(log_url)

        if parsed_log_url.scheme == "s3" and arn:
            s3 = boto.client("s3")
            report.add_boto_method_call(
                s3.list_objects_v2,
                Bucket=parsed_log_url.netloc,
                Prefix=parsed_log_url.params.rstrip("/"),
                MaxKeys=1,
            )
        elif parsed_log_url.scheme == "dbfs":
            response = dbx_client.list_dbfs_directory(parsed_log_url.geturl())
            if "error_code" not in response:
                report.append(
                    AccessReportLine(
                        name="Log Access",
                        status=AccessStatusCode.GREEN,
                        message=f"Can list objects at {parsed_log_url.geturl()}",
                    )
                )
            else:
                report.append(
                    AccessReportLine(
                        name="Log Access",
                        status=AccessStatusCode.RED,
                        message=f"Can list objects at {parsed_log_url.geturl()}",
                    )
                )
        else:
            report.append(
                AccessReportLine(
                    name="Log Access",
                    status=AccessStatusCode.RED,
                    message=f"scheme in {parsed_log_url.geturl()} is not supported",
                )
            )

    return report


def _get_cluster_report(
    cluster_id: str,
    cluster_tasks: List[dict],
    plan_type: str,
    compute_type: str,
    allow_incomplete: bool,
) -> Response[AWSDatabricksClusterReport]:
    # Cluster `terminated_time` can be a few seconds after the start of the next task in which
    # this may be executing.
    cluster_response = _wait_for_cluster_termination(cluster_id, poll_seconds=5)
    if cluster_response.error:
        return cluster_response

    cluster = cluster_response.result

    instances_response, timeline_response, volumes_response = _get_aws_cluster_info(cluster)

    if instances_response.error:
        if allow_incomplete:
            logger.warning(instances_response.error)
        else:
            return instances_response

    # The volumes data is less critical than instances, so allow
    # the cluster report to get created even if the volumes response
    # has an error.
    if volumes_response.error:
        logger.warning(volumes_response.error)
        volumes = []
    else:
        volumes = volumes_response.result

    if timeline_response.error:
        logger.warning(timeline_response.error)
        timelines = []
    else:
        timelines = timeline_response.result

    cluster_events = _get_all_cluster_events(cluster_id)
    return Response(
        result=AWSDatabricksClusterReport(
            plan_type=plan_type,
            compute_type=compute_type,
            cluster=cluster,
            cluster_events=cluster_events,
            volumes=volumes,
            tasks=cluster_tasks,
            instances=instances_response.result,
            instance_timelines=timelines,
        )
    )


if getattr(sync._databricks, "__claim", __name__) != __name__:
    raise RuntimeError(
        "Databricks modules for different cloud providers cannot be used in the same context"
    )

sync._databricks._get_cluster_report = _get_cluster_report
setattr(sync._databricks, "__claim", __name__)


def _load_aws_cluster_info(cluster: dict) -> Tuple[Response[dict], Response[dict]]:

    cluster_info = None
    cluster_id = None
    cluster_log_dest = _cluster_log_destination(cluster)

    if cluster_log_dest:
        (_, filesystem, bucket, base_prefix) = cluster_log_dest

        cluster_id = cluster["cluster_id"]
        spark_context_id = cluster["spark_context_id"]
        cluster_info_file_key = f"{base_prefix}/sync_data/{spark_context_id}/aws_cluster_info.json"

        cluster_info_file_response = None
        if filesystem == "s3":
            cluster_info_file_response = _get_aws_cluster_info_from_s3(
                bucket, cluster_info_file_key, cluster_id
            )
        elif filesystem == "dbfs":
            cluster_info_file_response = _get_cluster_instances_from_dbfs(cluster_info_file_key)

        cluster_info = (
            json.loads(cluster_info_file_response) if cluster_info_file_response else None
        )

    # If this cluster does not have the "Sync agent" configured, attempt a best-effort snapshot of the instances that
    #  are associated with this cluster
    if not cluster_info:
        try:
            ec2 = boto.client("ec2", region_name=DB_CONFIG.aws_region_name)
            instances = _get_ec2_instances(cluster_id, ec2)
            volumes = _get_ebs_volumes_for_instances(instances, ec2)

            cluster_info = {
                "instances": instances,
                "volumes": volumes,
            }

        except Exception as exc:
            logger.warning(exc)

    return cluster_info, cluster_id


def _get_aws_cluster_info(cluster: dict) -> Tuple[Response[dict], Response[dict], Response[dict]]:

    aws_region_name = DB_CONFIG.aws_region_name

    cluster_info, cluster_id = _load_aws_cluster_info(cluster)

    def missing_message(input: str) -> str:
        return (
            f"Unable to find any active or recently terminated {input} for cluster `{cluster_id}` in `{aws_region_name}`. "
            + "Please refer to the following documentation for options on how to address this - "
            + "https://docs.synccomputing.com/sync-gradient/integrating-with-gradient/databricks-workflows"
        )

    if not cluster_info or not cluster_info.get("instances"):
        instances_response = Response(error=DatabricksError(message=missing_message("instances")))
    else:
        instances_response = Response(result=cluster_info["instances"])

    if not cluster_info or not cluster_info.get("instance_timelines"):
        timeline_response = Response(error=DatabricksError(message=missing_message("timelines")))
    else:
        timeline_response = Response(result=cluster_info["instance_timelines"])

    if not cluster_info or not cluster_info.get("volumes"):
        volumes_response = Response(error=DatabricksError(message=missing_message("ebs volumes")))
    else:
        volumes_response = Response(result=cluster_info.get("volumes"))

    return instances_response, timeline_response, volumes_response


def _get_aws_cluster_info_from_s3(bucket: str, file_key: str, cluster_id):
    s3 = boto.client("s3")
    try:
        return s3.get_object(Bucket=bucket, Key=file_key)["Body"].read()
    except ClientError as err:
        logger.warning(f"Failed to retrieve cluster info from S3 with key, '{file_key}': {err}")


def monitor_cluster(
    cluster_id: str,
    polling_period: int = 20,
    cluster_report_destination_override: dict = None,
) -> None:
    cluster = get_default_client().get_cluster(cluster_id)
    spark_context_id = cluster.get("spark_context_id")

    while not spark_context_id:
        # This is largely just a convenience for when this command is run by someone locally
        logger.info("Waiting for cluster startup...")
        sleep(15)
        cluster = get_default_client().get_cluster(cluster_id)
        spark_context_id = cluster.get("spark_context_id")

    (log_url, filesystem, bucket, base_prefix) = _cluster_log_destination(cluster)
    if cluster_report_destination_override:
        filesystem = cluster_report_destination_override.get("filesystem", filesystem)
        base_prefix = cluster_report_destination_override.get("base_prefix", base_prefix)

    if log_url or cluster_report_destination_override:
        _monitor_cluster(
            (log_url, filesystem, bucket, base_prefix),
            cluster_id,
            spark_context_id,
            polling_period,
        )
    else:
        logger.warning("Unable to monitor cluster due to missing cluster log destination - exiting")


def _monitor_cluster(
    cluster_log_destination,
    cluster_id: str,
    spark_context_id: int,
    polling_period: int,
) -> None:

    (log_url, filesystem, bucket, base_prefix) = cluster_log_destination
    # If the event log destination is just a *bucket* without any sub-path, then we don't want to include
    #  a leading `/` in our Prefix (which will make it so that we never actually find the event log), so
    #  we make sure to re-strip our final Prefix
    file_key = f"{base_prefix}/sync_data/{spark_context_id}/aws_cluster_info.json".strip("/")

    aws_region_name = DB_CONFIG.aws_region_name
    ec2 = boto.client("ec2", region_name=aws_region_name)

    write_file = _define_write_file(file_key, filesystem, bucket)

    all_inst_by_id = {}
    active_timelines_by_id = {}
    retired_timelines = []
    recorded_volumes_by_id = {}
    while True:
        try:
            current_insts = _get_ec2_instances(cluster_id, ec2)
            recorded_volumes_by_id.update(
                {v["VolumeId"]: v for v in _get_ebs_volumes_for_instances(current_insts, ec2)}
            )

            # Record new (or overrwite) existing instances.
            # Separately record the ids of those that are in the "running" state.
            running_inst_ids = set({})
            for inst in current_insts:
                all_inst_by_id[inst["InstanceId"]] = inst
                if inst["State"]["Name"] == "running":
                    running_inst_ids.add(inst["InstanceId"])

            active_timelines_by_id, new_retired_timelines = _update_monitored_timelines(
                running_inst_ids, active_timelines_by_id
            )

            retired_timelines.extend(new_retired_timelines)
            all_timelines = retired_timelines + list(active_timelines_by_id.values())

            write_file(
                bytes(
                    json.dumps(
                        {
                            "instances": list(all_inst_by_id.values()),
                            "instance_timelines": all_timelines,
                            "volumes": list(recorded_volumes_by_id.values()),
                        },
                        cls=DefaultDateTimeEncoder,
                    ),
                    "utf-8",
                )
            )
        except Exception as e:
            logger.error(f"Exception encountered while polling cluster: {e}")

        sleep(polling_period)


def _define_write_file(file_key, filesystem, bucket):
    if filesystem == "file":
        file_path = Path(file_key)

        def ensure_path_exists(report_path: Path):
            logger.info(f"Ensuring path exists for {report_path}")
            report_path.parent.mkdir(parents=True, exist_ok=True)

        def write_file(body: bytes):
            logger.info("Saving state to local file")
            ensure_path_exists(file_path)
            with open(file_path, "wb") as f:
                f.write(body)

    elif filesystem == "s3":
        s3 = boto.client("s3")

        def write_file(body: bytes):
            logger.info("Saving state to S3")
            s3.put_object(Bucket=bucket, Key=file_key, Body=body)

    elif filesystem == "dbfs":
        path = format_dbfs_filepath(file_key)
        dbx_client = get_default_client()

        def write_file(body: bytes):
            logger.info("Saving state to DBFS")
            write_dbfs_file(path, body, dbx_client)

    else:
        raise ValueError(f"Unsupported filesystem: {filesystem}")
    return write_file


def _get_ec2_instances(cluster_id: str, ec2_client: "botocore.client.ec2") -> List[dict]:

    filters = [
        {"Name": "tag:Vendor", "Values": ["Databricks"]},
        {"Name": "tag:ClusterId", "Values": [cluster_id]},
    ]
    response = ec2_client.describe_instances(Filters=filters)
    reservations = response.get("Reservations", [])
    next_token = response.get("NextToken")

    while next_token:
        response = ec2_client.describe_instances(Filters=filters, NextToken=next_token)
        reservations += response.get("Reservations", [])
        next_token = response.get("NextToken")

    instances = []
    for res in reservations:
        for inst in res.get("Instances", []):
            instances.append(inst)
    logger.info(f"Identified {len(instances)} instances in cluster")

    return instances


def _get_ebs_volumes_for_instances(
    instances: List[dict], ec2_client: "botocore.client.ec2"
) -> List[dict]:
    """Get all ebs volumes associated with a list of instance reservations"""

    instance_ids = []
    if instances:
        for instance in instances:
            instance_ids.append(instance.get("InstanceId"))

    volumes = []
    if instance_ids:
        filters = [
            {"Name": "tag:Vendor", "Values": ["Databricks"]},
            {"Name": "attachment.instance-id", "Values": instance_ids},
        ]

        response = ec2_client.describe_volumes(Filters=filters)
        volumes = response.get("Volumes", [])
        next_token = response.get("NextToken")

        while next_token:
            response = ec2_client.describe_volumes(Filters=filters, NextToken=next_token)
            volumes += response.get("Volumes", [])
            next_token = response.get("NextToken")

    num_vol = len(volumes)
    logger.info(f"Identified {num_vol} ebs volumes in cluster")

    return volumes
