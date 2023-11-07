import logging
import os
import sys
from time import sleep
from typing import List, Type, TypeVar
from urllib.parse import urlparse

import orjson
from azure.common.credentials import get_cli_profile
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient

import sync._databricks
from sync._databricks import (
    _cluster_log_destination,
    _get_all_cluster_events,
    _get_cluster_instances_from_dbfs,
    _update_monitored_timelines,
    _wait_for_cluster_termination,
    apply_prediction,
    apply_project_recommendation,
    create_and_record_run,
    create_and_wait_for_run,
    create_cluster,
    create_prediction_for_run,
    create_run,
    create_submission_for_run,
    event_log_poll_duration_seconds,
    get_cluster,
    get_cluster_report,
    get_prediction_cluster,
    get_prediction_job,
    get_project_cluster,
    get_project_cluster_settings,
    get_project_job,
    get_recommendation_job,
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
from sync.models import (
    AccessReport,
    AccessReportLine,
    AccessStatusCode,
    AzureDatabricksClusterReport,
    DatabricksError,
    Response,
)
from sync.utils.dbfs import format_dbfs_filepath, write_dbfs_file

__all__ = [
    "get_access_report",
    "run_prediction",
    "run_and_record_job",
    "monitor_cluster",
    "create_cluster",
    "get_cluster",
    "create_prediction_for_run",
    "create_submission_for_run",
    "get_cluster_report",
    "record_run",
    "get_prediction_job",
    "get_prediction_cluster",
    "get_project_cluster",
    "get_project_job",
    "get_recommendation_job",
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
    "apply_prediction",
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

    try:
        DefaultAzureCredential().get_token("https://management.azure.com/.default")
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

    cluster_events = _get_all_cluster_events(cluster_id)
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


if getattr(sync._databricks, "__claim", __name__) != __name__:
    # Unless building documentation you can't load both databricks modules in the same program
    if not sys.argv[0].endswith("sphinx-build"):
        raise RuntimeError(
            "Databricks modules for different cloud providers cannot be used in the same context"
        )

sync._databricks._get_cluster_report = _get_cluster_report
setattr(sync._databricks, "__claim", __name__)


def _get_cluster_instances(cluster: dict) -> Response[dict]:
    cluster_instances = None

    cluster_log_dest = _cluster_log_destination(cluster)

    if cluster_log_dest:
        (_, filesystem, bucket, base_prefix) = cluster_log_dest

        cluster_id = cluster["cluster_id"]
        spark_context_id = cluster["spark_context_id"]
        cluster_instances_file_key = (
            f"{base_prefix}/sync_data/{spark_context_id}/cluster_instances.json"
        )

        cluster_instances_file_response = None
        if filesystem == "dbfs":
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

        resource_group_name = _get_databricks_resource_group_name()

        compute = _get_azure_client(ComputeManagementClient)
        if resource_group_name:
            vms = compute.virtual_machines.list(resource_group_name=resource_group_name)

            for vm in vms:
                compute.virtual_machines.instance_view(
                    resource_group_name=resource_group_name,
                )
        else:
            logger.warning("Failed to find Databricks managed resource group")
            vms = compute.virtual_machines.list_all()

        cluster_instances = {
            "instances": [
                vm.as_dict()
                for vm in vms
                if vm.tags.get("Vendor") == "Databricks"
                and vm.tags.get("ClusterId") == cluster["cluster_id"]
            ]
        }

    if not cluster_instances:
        no_instances_message = (
            f"Unable to find any active or recently terminated instances for cluster `{cluster_id}`. "
            + "Please refer to the following documentation for options on how to address this - "
            + "https://docs.synccomputing.com/sync-gradient/integrating-with-gradient/databricks-workflows"
        )
        return Response(error=DatabricksError(message=no_instances_message))

    return Response(result=cluster_instances)


def monitor_cluster(cluster_id: str, polling_period: int = 5) -> None:
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

    azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azure_logger.setLevel(logging.WARNING)

    if filesystem == "dbfs":
        path = format_dbfs_filepath(file_key)
        dbx_client = get_default_client()

        def write_file(body: bytes):
            logger.info("Saving state to DBFS")
            write_dbfs_file(path, body, dbx_client)

    resource_group_name = _get_databricks_resource_group_name()
    if not resource_group_name:
        logger.warning("Failed to find Databricks managed resource group")

    compute = _get_azure_client(ComputeManagementClient)

    all_vms_by_id = {}
    active_timelines_by_id = {}
    retired_timelines = []
    while True:
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
                orjson.dumps(
                    {
                        "instances": list(all_vms_by_id),
                        "timelines": all_timelines,
                    }
                )
            )

        except Exception as e:
            logger.error(f"Exception encountered while polling cluster: {e}")
        sleep(polling_period)


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


def _get_azure_client(azure_client_class: Type[AzureClient]) -> AzureClient:
    global _azure_subscription_id
    if not _azure_subscription_id:
        _azure_subscription_id = _get_azure_subscription_id()

    global _azure_credential
    if not _azure_credential:
        _azure_credential = DefaultAzureCredential()

    return azure_client_class(_azure_credential, _azure_subscription_id)


def _get_azure_subscription_id():
    return os.getenv("AZURE_SUBSCRIPTION_ID") or get_cli_profile().get_login_credentials()[1]


def _get_running_vms_by_id(
    compute: AzureClient, resource_group_name: str | None, cluster_id: str
) -> dict[str, dict]:

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
