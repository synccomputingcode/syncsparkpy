import logging
from time import sleep
from urllib.parse import urlparse

import boto3 as boto
import orjson
from botocore.exceptions import ClientError

import sync._databricks
from sync._databricks import (
    _cluster_log_destination,
    _get_all_cluster_events,
    _get_cluster_instances_from_dbfs,
    _wait_for_cluster_termination,
    create_and_record_run,
    create_and_wait_for_run,
    create_cluster,
    create_prediction_for_run,
    create_run,
    event_log_poll_duration_seconds,
    get_cluster,
    get_cluster_report,
    get_prediction_cluster,
    get_prediction_job,
    get_project_cluster,
    get_project_cluster_settings,
    get_project_job,
    record_run,
    run_and_record_job,
    run_and_record_job_object,
    run_and_record_prediction_job,
    run_and_record_project_job,
    run_job_object,
    run_prediction,
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

__all__ = [
    "get_access_report",
    "run_prediction",
    "run_and_record_job",
    "create_prediction_for_run",
    "get_cluster_report",
    "monitor_cluster",
    "create_cluster",
    "get_cluster",
    "record_run",
    "get_prediction_job",
    "get_prediction_cluster",
    "get_project_job",
    "get_project_cluster",
    "get_project_cluster_settings",
    "run_job_object",
    "create_run",
    "run_and_record_prediction_job",
    "run_and_record_project_job",
    "run_and_record_job_object",
    "create_and_record_run",
    "wait_for_and_record_run",
    "create_and_wait_for_run",
    "wait_for_final_run_status",
    "wait_for_run_and_cluster",
    "terminate_cluster",
    "event_log_poll_duration_seconds",
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
    cluster_id: str, plan_type: str, compute_type: str, allow_incomplete: bool
) -> Response[AWSDatabricksClusterReport]:
    # Cluster `terminated_time` can be a few seconds after the start of the next task in which
    # this may be executing.
    cluster_response = _wait_for_cluster_termination(cluster_id, poll_seconds=5)
    if cluster_response.error:
        return cluster_response

    cluster = cluster_response.result

    instances = _get_cluster_instances(cluster)
    if instances.error:
        if allow_incomplete:
            logger.warning(instances.error)
        else:
            return instances

    cluster_events = _get_all_cluster_events(cluster_id)
    return Response(
        result=AWSDatabricksClusterReport(
            plan_type=plan_type,
            compute_type=compute_type,
            cluster=cluster,
            cluster_events=cluster_events,
            instances=instances.result,
        )
    )


sync._databricks._get_cluster_report = _get_cluster_report


def _get_cluster_instances(cluster: dict) -> Response[dict]:
    cluster_instances = None
    aws_region_name = DB_CONFIG.aws_region_name

    cluster_log_dest = _cluster_log_destination(cluster)

    if cluster_log_dest:
        (_, filesystem, bucket, base_prefix) = cluster_log_dest

        cluster_id = cluster["cluster_id"]
        spark_context_id = cluster["spark_context_id"]
        cluster_instances_file_key = (
            f"{base_prefix}/sync_data/{spark_context_id}/cluster_instances.json"
        )

        cluster_instances_file_response = None
        if filesystem == "s3":
            cluster_instances_file_response = _get_cluster_instances_from_s3(
                bucket, cluster_instances_file_key, cluster_id
            )
        elif filesystem == "dbfs":
            cluster_instances_file_response = _get_cluster_instances_from_dbfs(
                cluster_instances_file_key
            )

        cluster_instances = (
            orjson.loads(cluster_instances_file_response)
            if cluster_instances_file_response
            else None
        )

    # If this cluster does not have the "Sync agent" configured, attempt a best-effort snapshot of the instances that
    #  are associated with this cluster
    if not cluster_instances:
        ec2 = boto.client("ec2", region_name=aws_region_name)
        try:
            cluster_instances = ec2.describe_instances(
                Filters=[
                    {"Name": "tag:Vendor", "Values": ["Databricks"]},
                    {"Name": "tag:ClusterId", "Values": [cluster_id]},
                    # {'Name': 'tag:JobId', 'Values': []}
                ]
            )
        except Exception as exc:
            logger.warning(exc)

    if not cluster_instances or not cluster_instances["Reservations"]:
        no_instances_message = (
            f"Unable to find any active or recently terminated instances for cluster `{cluster_id}` in `{aws_region_name}`. "
            + "Please refer to the following documentation for options on how to address this - "
            + "https://docs.synccomputing.com/sync-gradient/integrating-with-gradient/databricks-workflows"
        )
        return Response(error=DatabricksError(message=no_instances_message))

    return Response(result=cluster_instances)


def _get_cluster_instances_from_s3(bucket: str, file_key: str, cluster_id):
    s3 = boto.client("s3")
    try:
        return s3.get_object(Bucket=bucket, Key=file_key)["Body"].read()
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchKey":
            logger.warning(
                f"Could not find sync_data/cluster_instances.json for cluster: {cluster_id}"
            )
        else:
            logger.error(
                f"Unexpected error encountered while attempting to fetch sync_data/cluster_instances.json: {ex}"
            )


def monitor_cluster(cluster_id: str, polling_period: int = 30) -> None:
    cluster = get_default_client().get_cluster(cluster_id)
    spark_context_id = cluster.get("spark_context_id")

    while not spark_context_id:
        # This is largely just a convenience for when this command is run by someone locally
        logger.info("Waiting for cluster startup...")
        sleep(30)
        cluster = get_default_client().get_cluster(cluster_id)
        spark_context_id = cluster.get("spark_context_id")

    (log_url, filesystem, bucket, base_prefix) = _cluster_log_destination(cluster)
    if log_url:
        _monitor_cluster(
            (log_url, filesystem, bucket, base_prefix), cluster_id, spark_context_id, polling_period
        )
    else:
        logger.warning("Unable to monitor cluster due to missing cluster log destination - exiting")


def _monitor_cluster(
    cluster_log_destination, cluster_id: str, spark_context_id: int, polling_period: int
) -> None:
    (log_url, filesystem, bucket, base_prefix) = cluster_log_destination
    # If the event log destination is just a *bucket* without any sub-path, then we don't want to include
    #  a leading `/` in our Prefix (which will make it so that we never actually find the event log), so
    #  we make sure to re-strip our final Prefix
    file_key = f"{base_prefix}/sync_data/{spark_context_id}/cluster_instances.json".strip("/")

    aws_region_name = DB_CONFIG.aws_region_name
    ec2 = boto.client("ec2", region_name=aws_region_name)

    if filesystem == "s3":
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

    previous_instances = {}
    while True:
        try:
            instances = ec2.describe_instances(
                Filters=[
                    {"Name": "tag:Vendor", "Values": ["Databricks"]},
                    {"Name": "tag:ClusterId", "Values": [cluster_id]},
                    # {'Name': 'tag:JobId', 'Values': []}
                ]
            )

            new_instances = [res for res in instances["Reservations"]]
            new_instance_id_to_reservation = dict(
                zip(
                    (res["Instances"][0]["InstanceId"] for res in new_instances),
                    new_instances,
                )
            )

            old_instances = [res for res in previous_instances.get("Reservations", [])]
            old_instance_id_to_reservation = dict(
                zip(
                    (res["Instances"][0]["InstanceId"] for res in old_instances),
                    old_instances,
                )
            )

            old_instance_ids = set(old_instance_id_to_reservation)
            new_instance_ids = set(new_instance_id_to_reservation)

            # If we have the exact same set of instances, prefer the new set...
            if old_instance_ids == new_instance_ids:
                instances = {"Reservations": new_instances}
            else:
                # Otherwise, update old references and include any new instances in the list
                newly_added_instance_ids = new_instance_ids.difference(old_instance_ids)
                updated_instance_ids = new_instance_ids.intersection(old_instance_ids)
                removed_instance_ids = old_instance_ids.difference(new_instance_ids)

                removed_instances = [
                    old_instance_id_to_reservation[id] for id in removed_instance_ids
                ]
                updated_instances = [
                    new_instance_id_to_reservation[id] for id in updated_instance_ids
                ]
                new_instances = [
                    new_instance_id_to_reservation[id] for id in newly_added_instance_ids
                ]

                instances = {
                    "Reservations": [*removed_instances, *updated_instances, *new_instances]
                }

            write_file(orjson.dumps(instances))

            previous_instances = instances
        except Exception as e:
            logger.error(f"Exception encountered while polling cluster: {e}")

        sleep(polling_period)
