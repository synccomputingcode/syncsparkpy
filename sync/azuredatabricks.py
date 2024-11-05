import json
import logging
import os
import sys
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional, Type, TypeVar, Union
from urllib.parse import urlparse

from azure.common.credentials import get_cli_profile
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient

import sync._databricks
from sync._databricks import (
    _cluster_log_destination,
    _get_cluster_instances_from_dbfs,
    _update_monitored_timelines,
    _wait_for_cluster_termination,
    apply_project_recommendation,
    create_and_record_run,
    create_and_wait_for_run,
    create_cluster,
    create_run,
    create_submission_for_run,
    create_submission_with_cluster_info,
    get_all_cluster_events,
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
from sync.models import (
    AccessReport,
    AccessReportLine,
    AccessStatusCode,
    AzureDatabricksClusterReport,
    DatabricksComputeType,
    DatabricksError,
    DatabricksPlanType,
    Response,
)
from sync.utils.dbfs import format_dbfs_filepath, write_dbfs_file
from sync.utils.json import DefaultDateTimeEncoder

__all__ = [
    "apply_project_recommendation",
    "create_and_record_run",
    "create_and_wait_for_run",
    "create_cluster",
    "create_run",
    "create_submission_for_run",
    "create_submission_with_cluster_info",
    "get_access_report",
    "get_all_cluster_events",
    "get_cluster",
    "get_cluster_report",
    "get_project_cluster",
    "get_project_cluster",
    "get_project_cluster_settings",
    "get_project_job",
    "get_recommendation_job",
    "handle_successful_job_run",
    "monitor_cluster",
    "monitor_once",
    "record_run",
    "run_and_record_job",
    "run_and_record_job_object",
    "run_and_record_project_job",
    "run_job_object",
    "terminate_cluster",
    "wait_for_and_record_run",
    "wait_for_final_run_status",
    "wait_for_run_and_cluster",
]

logger = logging.getLogger(__name__)


def get_access_report(log_url: Optional[str] = None) -> AccessReport:
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

    try:
        get_azure_credential().get_token("https://management.azure.com/.default")
        report.append(
            AccessReportLine(
                name="Azure Authentication",
                status=AccessStatusCode.GREEN,
                message="Retrieved management token ",
            )
        )
    except ClientAuthenticationError as err:
        report.append(
            AccessReportLine(
                name="Azure Authentication",
                status=AccessStatusCode.RED,
                message=f"Authenticated as '{err}'",
            )
        )

    subscription_id = _get_azure_subscription_id()
    if subscription_id:
        report.append(
            AccessReportLine(
                name="Azure Authentication",
                status=AccessStatusCode.GREEN,
                message=f"Subscription ID found: {subscription_id}",
            )
        )
    else:
        report.append(
            AccessReportLine(
                name="Azure Authentication",
                status=AccessStatusCode.RED,
                message="Subscription ID not found",
            )
        )

    if log_url:
        parsed_log_url = urlparse(log_url)

        if parsed_log_url.scheme == "dbfs":
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
) -> Response[AzureDatabricksClusterReport]:
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

    cluster_events = get_all_cluster_events(cluster_id)
    return Response(
        result=AzureDatabricksClusterReport(
            plan_type=plan_type,
            compute_type=compute_type,
            cluster=cluster,
            cluster_events=cluster_events,
            tasks=cluster_tasks,
            instances=instances.result.get("instances"),
            instance_timelines=instances.result.get("timelines"),
        )
    )


def _create_cluster_report(
    cluster: dict,
    cluster_info: dict,
    cluster_activity_events: dict,
    tasks: List[dict],
    plan_type: DatabricksPlanType,
    compute_type: DatabricksComputeType,
) -> AzureDatabricksClusterReport:
    return AzureDatabricksClusterReport(
        plan_type=plan_type,
        compute_type=compute_type,
        cluster=cluster,
        cluster_events=cluster_activity_events,
        tasks=tasks,
        instances=cluster_info.get("instances"),
        instance_timelines=cluster_info.get("timelines"),
    )


if getattr(sync._databricks, "__claim", __name__) != __name__:
    # Unless building documentation you can't load both databricks modules in the same program
    if not sys.argv[0].endswith("sphinx-build"):
        raise RuntimeError(
            "Databricks modules for different cloud providers cannot be used in the same context"
        )

sync._databricks._get_cluster_report = _get_cluster_report
sync._databricks._create_cluster_report = _create_cluster_report
setattr(sync._databricks, "__claim", __name__)


def _get_cluster_instances(cluster: dict) -> Response[dict]:
    cluster_instances = None

    cluster_log_dest = _cluster_log_destination(cluster)

    if cluster_log_dest:
        (_, filesystem, _bucket, base_prefix) = cluster_log_dest

        cluster_id = cluster["cluster_id"]
        spark_context_id = cluster["spark_context_id"]
        cluster_instances_file_key = (
            f"{base_prefix}/sync_data/{spark_context_id}/azure_cluster_info.json"
        )

        cluster_instances_file_response = None
        if filesystem == "dbfs":
            cluster_instances_file_response = _get_cluster_instances_from_dbfs(
                cluster_instances_file_key
            )

        cluster_instances = (
            json.loads(cluster_instances_file_response) if cluster_instances_file_response else None
        )

        if not cluster_instances:
            no_instances_message = f"Unable to find cluster information from Sync's cluster monitor for cluster `{cluster_id}`. "
            return Response(error=DatabricksError(message=no_instances_message))

    return Response(result=cluster_instances)


def monitor_cluster(
    cluster_id: str,
    polling_period: int = 20,
    cluster_report_destination_override: Optional[dict] = None,
    kill_on_termination: bool = False,
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
    write_function = None
    if cluster_report_destination_override:
        filesystem = cluster_report_destination_override.get("filesystem", filesystem)
        base_prefix = cluster_report_destination_override.get("base_prefix", base_prefix)
        write_function = cluster_report_destination_override.get("write_function")

    if log_url:
        _monitor_cluster(
            (log_url, filesystem, bucket, base_prefix),
            cluster_id,
            spark_context_id,
            polling_period,
            kill_on_termination,
            write_function,
        )
    else:
        logger.warning("Unable to monitor cluster due to missing cluster log destination - exiting")


def _monitor_cluster(
    cluster_log_destination,
    cluster_id: str,
    spark_context_id: int,
    polling_period: int,
    kill_on_termination: bool = False,
    write_function=None,
) -> None:
    (_log_url, filesystem, _bucket, base_prefix) = cluster_log_destination
    # If the event log destination is just a *bucket* without any sub-path, then we don't want to include
    #  a leading `/` in our Prefix (which will make it so that we never actually find the event log), so
    #  we make sure to re-strip our final Prefix
    file_key = f"{base_prefix}/sync_data/{spark_context_id}/azure_cluster_info.json".strip("/")

    azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azure_logger.setLevel(logging.WARNING)

    write_file = _define_write_file(file_key, filesystem, write_function)

    resource_group_name = _get_databricks_resource_group_name()
    if not resource_group_name:
        logger.warning("Failed to find Databricks managed resource group")

    compute = _get_azure_client(ComputeManagementClient)

    all_vms_by_id = {}
    active_timelines_by_id = {}
    retired_timelines = []

    while_condition = True
    while while_condition:
        try:
            running_vms_by_id = _get_running_vms_by_id(compute, resource_group_name, cluster_id)

            for vm in running_vms_by_id.values():
                all_vms_by_id[vm["name"]] = vm

            active_timelines_by_id, new_retired_timelines = _update_monitored_timelines(
                set(running_vms_by_id.keys()), active_timelines_by_id
            )
            retired_timelines.extend(new_retired_timelines)
            all_timelines = retired_timelines + list(active_timelines_by_id.values())

            write_file(
                bytes(
                    json.dumps(
                        {
                            "instances": list(all_vms_by_id.values()),
                            "timelines": all_timelines,
                        },
                        cls=DefaultDateTimeEncoder,
                    ),
                    "utf-8",
                )
            )

            if kill_on_termination:
                cluster_state = get_default_client().get_cluster(cluster_id).get("state")
                if cluster_state == "TERMINATED":
                    while_condition = False
        except Exception as e:
            logger.error(f"Exception encountered while polling cluster: {e}")
        sleep(polling_period)


def monitor_once(cluster_id: str, in_progress_cluster=None):
    if in_progress_cluster is None:
        in_progress_cluster = {}
    all_vms_by_id = in_progress_cluster.get("all_vms_by_id") or {}
    active_timelines_by_id = in_progress_cluster.get("active_timelines_by_id") or {}
    retired_timelines = in_progress_cluster.get("retired_timelines") or []

    resource_group_name = _get_databricks_resource_group_name()
    if not resource_group_name:
        logger.warning("Failed to find Databricks managed resource group")

    compute = _get_azure_client(ComputeManagementClient)

    running_vms_by_id = _get_running_vms_by_id(compute, resource_group_name, cluster_id)

    for vm in running_vms_by_id.values():
        all_vms_by_id[vm["name"]] = vm

    active_timelines_by_id, new_retired_timelines = _update_monitored_timelines(
        set(running_vms_by_id.keys()), active_timelines_by_id
    )
    retired_timelines.extend(new_retired_timelines)

    return {
        "all_vms_by_id": all_vms_by_id,
        "active_timelines_by_id": active_timelines_by_id,
        "retired_timelines": retired_timelines,
    }


def _define_write_file(file_key, filesystem, write_function):
    if filesystem == "lambda":

        def write_file(body: bytes):
            logger.info("Using custom lambda function to write data")
            write_function(body)

    elif filesystem == "file":
        file_path = Path(file_key)

        def ensure_path_exists(report_path: Path):
            logger.info(f"Ensuring path exists for {report_path}")
            report_path.parent.mkdir(parents=True, exist_ok=True)

        def write_file(body: bytes):
            logger.info("Saving state to local file")
            ensure_path_exists(file_path)
            with open(file_path, "wb") as f:
                f.write(body)

    elif filesystem == "dbfs":
        path = format_dbfs_filepath(file_key)
        dbx_client = get_default_client()

        def write_file(body: bytes):
            logger.info("Saving state to DBFS")
            write_dbfs_file(path, body, dbx_client)

    else:
        raise ValueError(f"Unsupported filesystem: {filesystem}")
    return write_file


def _get_databricks_resource_group_name() -> str:
    resources = _get_azure_client(ResourceManagementClient)

    for workspace_item in resources.resources.list(
        filter="resourceType eq 'Microsoft.Databricks/workspaces'"
    ):
        workspace = resources.resources.get_by_id(workspace_item.id, "2023-02-01")
        if workspace.properties["workspaceUrl"] == get_default_client().get_host().netloc.decode():
            return workspace.properties["managedResourceGroupId"].split("/")[-1]


_azure_credential = None
_azure_subscription_id = None

AzureClient = TypeVar("AzureClient")


def get_azure_credential() -> Union[ClientSecretCredential, DefaultAzureCredential]:
    global _azure_credential
    if _azure_credential is None:
        _azure_credential = DefaultAzureCredential()
    return _azure_credential


def set_azure_client_credentials(
    azure_subscription_id: str, azure_credential: ClientSecretCredential
):
    global _azure_subscription_id
    if _azure_subscription_id is not None:
        raise RuntimeError("Azure client credentials already set, cannot reset subscription id")
    _azure_subscription_id = azure_subscription_id

    global _azure_credential
    if _azure_credential is not None:
        raise RuntimeError("Azure client credentials already set, cannot reset credentials")
    _azure_credential = azure_credential


def _get_azure_client(azure_client_class: Type[AzureClient]) -> AzureClient:
    global _azure_subscription_id
    if not _azure_subscription_id:
        _azure_subscription_id = _get_azure_subscription_id()

    global _azure_credential
    if not _azure_credential:
        _azure_credential = get_azure_credential()

    return azure_client_class(_azure_credential, _azure_subscription_id)


def _get_azure_subscription_id():
    global _azure_subscription_id
    subscription_id = (
        _azure_subscription_id
        or os.getenv("AZURE_SUBSCRIPTION_ID")
        or get_cli_profile().get_login_credentials()[1]
    )
    return subscription_id


def _get_running_vms_by_id(
    compute: AzureClient, resource_group_name: Optional[str], cluster_id: str
) -> Dict[str, dict]:
    if resource_group_name:
        vms = compute.virtual_machines.list(resource_group_name=resource_group_name)
    else:
        vms = compute.virtual_machines.list_all()

    current_vms = [
        vm.as_dict()
        for vm in vms
        if vm.tags.get("Vendor") == "Databricks" and vm.tags.get("ClusterId") == cluster_id
    ]

    # A separate api call is required for each vm to see its power state.
    # Use a conservative default of assuming the vm is running if the resource group name
    # is missing and the api call can't be made.
    running_vms_by_id = {}
    for vm in current_vms:
        if resource_group_name:
            vm_state = compute.virtual_machines.instance_view(
                resource_group_name=resource_group_name, vm_name=vm["name"]
            )
            if len(vm_state.statuses) > 1 and vm_state.statuses[1].code == "PowerState/running":
                running_vms_by_id[vm["name"]] = vm
        else:
            running_vms_by_id[vm["name"]] = vm

    return running_vms_by_id
