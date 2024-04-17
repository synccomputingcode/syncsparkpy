import json
from urllib.parse import urlparse
from uuid import uuid4

import click

from sync.api import workspace
from sync.cli.util import OPTIONAL_DEFAULT, validate_project
from sync.config import API_KEY, DB_CONFIG
from sync.models import (
    AwsHostedIAMInstructions,
    AwsRegionEnum,
    ComputeProvider,
    CreateWorkspaceConfig,
    DatabricksPlanType,
    HostingType,
    UpdateWorkspaceConfig,
    WorkspaceCollectionTypeEnum,
)
from sync.utils.json import DateTimeEncoderNaiveUTCDropMicroseconds


@click.group
def workspaces():
    """Sync project commands"""
    pass


@workspaces.command
@click.argument("workspace-id")
@click.option(
    "--instance-profile-arn",
    help="Instance profile to apply to Sync reporting job and on-boarded Databricks jobs",
)
@click.option(
    "--databricks-plan-type",
    type=click.Choice(DatabricksPlanType),
    default=DatabricksPlanType.STANDARD,
)
@click.option(
    "--databricks-webhook-id",
    help="UUID of Sync Computing notification destination in the Databricks workspace",
)
@click.option(
    "--aws-region",
    type=click.Choice(AwsRegionEnum),
    help="Workspace region",
)
@click.option(
    "--cluster-policy-id",
    help="ID of cluster policy to apply to Sync job reporting cluster",
)
@click.option(
    "--hosting-type",
    type=click.Choice(HostingType),
    help="Indicates where the workspace is hosted",
)
@click.option(
    "--compute-provider",
    type=click.Choice(ComputeProvider),
    help="Indicates the compute provider for the workspace",
)
@click.option(
    "--aws-iam-role-arn",
    help="AWS IAM Role ARN",
)
@click.option(
    "--azure-subscription-id",
    help="Azure Subscription ID",
)
@click.option(
    "--azure-client-id",
    help="Azure Client ID",
)
@click.option(
    "--azure-tenant-id",
    help="Azure Tenant ID",
)
@click.option(
    "--azure-client-secret",
    help="Azure Client Secret",
)
def create_workspace_config(
    workspace_id: str,
    instance_profile_arn: str = None,
    databricks_plan_type: str = None,
    databricks_webhook_id: str = None,
    aws_region: str = None,
    cluster_policy_id: str = None,
    hosting_type: HostingType = None,
    compute_provider: ComputeProvider = None,
    aws_iam_role_arn: str = None,
    azure_subscription_id: str = None,
    azure_client_id: str = None,
    azure_tenant_id: str = None,
    azure_client_secret: str = None,
):
    databricks_host, databricks_token = _prompt_databricks_fields()

    sync_api_key_id, sync_api_key_secret = _prompt_sync_api_fields()

    compute_provider: ComputeProvider = _determine_compute_provider(
        compute_provider, databricks_host
    )

    aws_region = (
        aws_region if aws_region is None and compute_provider == ComputeProvider.AWS else None
    )

    hosting_type = _prompt_hosting_type(hosting_type)
    collection_type = _determine_collection_type(hosting_type)
    aws_external_id = str(uuid4())

    if hosting_type == HostingType.SELF_HOSTED and compute_provider == ComputeProvider.AWS:
        instance_profile_arn = instance_profile_arn or click.prompt("AWS Instance profile ARN")

    if hosting_type == HostingType.SYNC_HOSTED:
        if compute_provider == ComputeProvider.AWS:
            aws_iam_role_arn = _prompt_aws_arn_info(aws_external_id, aws_iam_role_arn)
        elif compute_provider == ComputeProvider.AZURE:
            (
                azure_client_id,
                azure_client_secret,
                azure_subscription_id,
                azure_tenant_id,
            ) = _handle_azure_provider(
                azure_client_id, azure_client_secret, azure_subscription_id, azure_tenant_id
            )

    workspace_config = CreateWorkspaceConfig(
        workspace_id=workspace_id,
        databricks_host=databricks_host,
        databricks_token=databricks_token,
        sync_api_key_id=sync_api_key_id,
        sync_api_key_secret=sync_api_key_secret,
        collection_type=collection_type,
        compute_provider=compute_provider,
        instance_profile_arn=instance_profile_arn,
        databricks_plan_type=databricks_plan_type,
        webhook_id=databricks_webhook_id,
        aws_region=aws_region,
        aws_iam_role_arn=aws_iam_role_arn,
        cluster_policy_id=cluster_policy_id,
        external_id=aws_external_id,
        azure_tenant_id=azure_tenant_id,
        azure_client_id=azure_client_id,
        azure_client_secret=azure_client_secret,
        azure_subscription_id=azure_subscription_id,
    )
    response = workspace.create_workspace_config(workspace_config)

    config = response.result
    if config:
        click.echo(json.dumps(config, indent=2, cls=DateTimeEncoderNaiveUTCDropMicroseconds))
    else:
        click.echo(str(response.error), err=True)


def _prompt_aws_arn_info(aws_external_id, aws_iam_role_arn):
    instructions = AwsHostedIAMInstructions(external_id=aws_external_id)
    click.echo(click.style(text=instructions.step_1_prompt, bold=True))
    click.echo(instructions.step_1_value)
    click.echo(click.style(text=instructions.step_2_prompt, bold=True))
    click.echo(instructions.step_2_value)
    aws_iam_role_arn = aws_iam_role_arn or click.prompt(
        "AWS IAM Role ARN",
    )
    return aws_iam_role_arn


def _prompt_hosting_type(hosting_type):
    hosting_type = hosting_type or click.prompt(
        "Hosting type (sync-hosted or self-hosted)",
        type=click.Choice([h.value for h in HostingType]),
        default=HostingType.SYNC_HOSTED,
        show_default=True,
    )
    return hosting_type


def _prompt_sync_api_fields():
    sync_api_key_id = click.prompt("Sync API key ID", default=getattr(API_KEY, "id", None))
    sync_api_key_secret = click.prompt(
        "Sync API key secret",
        default=getattr(API_KEY, "secret", None),
        hide_input=True,
        show_default=False,
    )
    return sync_api_key_id, sync_api_key_secret


def _prompt_databricks_fields():
    databricks_host = click.prompt(
        "Databricks host (prefix with https://)", default=getattr(DB_CONFIG, "host", None)
    )
    databricks_token = click.prompt(
        "Databricks token",
        default=getattr(DB_CONFIG, "token", None),
        hide_input=True,
        show_default=False,
    )
    return databricks_host, databricks_token


def _determine_collection_type(hosting_type):
    return (
        WorkspaceCollectionTypeEnum.HOSTED
        if hosting_type == HostingType.SYNC_HOSTED
        else WorkspaceCollectionTypeEnum.REMOTE
    )


def _handle_azure_provider(
    azure_client_id, azure_client_secret, azure_subscription_id, azure_tenant_id
):
    azure_subscription_id = azure_subscription_id or click.prompt("Azure Subscription ID")
    azure_client_id = azure_client_id or click.prompt("Azure Client ID")
    azure_tenant_id = azure_tenant_id or click.prompt("Azure Tenant ID")
    azure_client_secret = azure_client_secret or click.prompt("Azure Client Secret")
    return azure_client_id, azure_client_secret, azure_subscription_id, azure_tenant_id


def _get_provider_from_url(url: str) -> str:
    netloc = urlparse(url).netloc
    if netloc.endswith(".azuredatabricks.net"):
        return ComputeProvider.AZURE
    elif netloc.endswith(".cloud.databricks.com"):
        return ComputeProvider.AWS


def _determine_compute_provider(compute_provider, databricks_host):
    return (
        compute_provider
        or _get_provider_from_url(databricks_host)
        or click.prompt(
            "Compute provider (aws or azure)",
            type=click.Choice(ComputeProvider),
            default=ComputeProvider.AWS,
            show_default=True,
        )
    )


@workspaces.command
@click.argument("workspace-id")
def get_workspace_config(workspace_id: str):
    config_response = workspace.get_workspace_config(workspace_id)
    config = config_response.result
    if config:
        click.echo(
            json.dumps(
                config,
                indent=2,
                cls=DateTimeEncoderNaiveUTCDropMicroseconds,
            )
        )
    else:
        click.echo(str(config_response.error), err=True)


@workspaces.command
def list_workspace_configs():
    configs_response = workspace.get_workspace_configs()
    configs = configs_response.result
    if configs:
        click.echo_via_pager(f"{c['workspace_id']}\n" for c in configs)
    else:
        click.echo(str(configs_response.error), err=True)


@workspaces.command
@click.argument("workspace-id")
@click.option(
    "--instance-profile-arn",
    help="Instance profile to apply to Sync reporting job and on-boarded Databricks jobs",
)
@click.option(
    "--databricks-webhook-id",
    help="UUID of Sync Computing notification destination in the Databricks workspace",
)
@click.option("--databricks-plan-type", type=click.Choice(DatabricksPlanType))
@click.option(
    "--aws-region",
    help="Workspace region",
)
@click.option(
    "--cluster-policy-id",
    help="ID of cluster policy to apply to Sync job reporting cluster",
)
@click.option(
    "--aws-iam-role-arn",
    help="AWS IAM Role ARN",
)
@click.option(
    "--azure-subscription-id",
    help="Azure Subscription ID",
)
@click.option(
    "--azure-client-id",
    help="Azure Client ID",
)
@click.option(
    "--azure-tenant-id",
    help="Azure Tenant ID",
)
@click.option(
    "--azure-client-secret",
    help="Azure Client Secret",
)
def update_workspace_config(
    workspace_id: str,
    instance_profile_arn: str = None,
    databricks_plan_type: str = None,
    databricks_webhook_id: str = None,
    aws_region: str = None,
    cluster_policy_id: str = None,
    aws_iam_role_arn: str = None,
    azure_subscription_id: str = None,
    azure_client_id: str = None,
    azure_tenant_id: str = None,
    azure_client_secret: str = None,
):
    current_config_response = workspace.get_workspace_config(workspace_id)
    current_config = current_config_response.result
    if current_config:
        databricks_host = click.prompt(
            "Databricks host (prefix with https://, optional)",
            default=OPTIONAL_DEFAULT,
            show_default=False,
        )
        databricks_token = click.prompt(
            "Databricks token (optional)", default=OPTIONAL_DEFAULT, show_default=False
        )
        sync_api_key_id = click.prompt(
            "Sync API key ID (optional)", default=OPTIONAL_DEFAULT, show_default=False
        )
        sync_api_key_secret = click.prompt(
            "Sync API key secret (optional)", default=OPTIONAL_DEFAULT, show_default=False
        )
        update_configuration = UpdateWorkspaceConfig(
            workspace_id=workspace_id,
            databricks_host=databricks_host if databricks_host != OPTIONAL_DEFAULT else None,
            databricks_token=databricks_token if databricks_token != OPTIONAL_DEFAULT else None,
            sync_api_key_id=sync_api_key_id if sync_api_key_id != OPTIONAL_DEFAULT else None,
            sync_api_key_secret=(
                sync_api_key_secret if sync_api_key_secret != OPTIONAL_DEFAULT else None
            ),
            instance_profile_arn=instance_profile_arn,
            webhook_id=databricks_webhook_id,
            databricks_plan_type=databricks_plan_type,
            aws_region=aws_region,
            cluster_policy_id=cluster_policy_id,
            aws_iam_role_arn=aws_iam_role_arn,
            azure_subscription_id=azure_subscription_id,
            azure_client_id=azure_client_id,
            azure_tenant_id=azure_tenant_id,
            azure_client_secret=azure_client_secret,
        )

        update_config_response = workspace.update_workspace_config(update_configuration)
        config = update_config_response.result
        if config:
            click.echo(
                json.dumps(
                    config,
                    indent=2,
                    cls=DateTimeEncoderNaiveUTCDropMicroseconds,
                )
            )
        else:
            click.echo(
                f"Failed to update workspace configuration. {update_config_response.error}",
                err=True,
            )
    else:
        click.echo(str(current_config_response.error), err=True)


@workspaces.command
@click.argument("workspace-id")
def delete_workspace_config(workspace_id: str):
    response = workspace.delete_workspace_config(workspace_id)
    result = response.result
    if result:
        click.echo(result)
    else:
        click.echo(str(response.error), err=True)


@workspaces.command
@click.argument("workspace-id")
def reset_webhook_creds(workspace_id: str):
    response = workspace.reset_webhook_creds(workspace_id)
    result = response.result
    if result:
        click.echo(
            json.dumps(
                result,
                indent=2,
                cls=DateTimeEncoderNaiveUTCDropMicroseconds,
            )
        )
    else:
        click.echo(str(response.error), err=True)


@workspaces.command
@click.argument("workspace-id")
def apply_workspace_config(workspace_id: str):
    response = workspace.apply_workspace_config(workspace_id)
    result = response.result
    if result:
        click.echo(result)
    else:
        click.echo(str(response.error), err=True)


@workspaces.command
@click.argument("workspace-id")
@click.argument("job-id")
@click.argument("project", callback=validate_project)
def onboard_job(workspace_id: str, job_id: str, project: dict):
    response = workspace.onboard_job(workspace_id, job_id, project["id"])
    result = response.result
    if result:
        click.echo(result)
    else:
        click.echo(str(response.error), err=True)
