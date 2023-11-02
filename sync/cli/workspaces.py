import click
import orjson

from sync.api import workspace
from sync.cli.util import OPTIONAL_DEFAULT, validate_project
from sync.config import API_KEY, DB_CONFIG
from sync.models import DatabricksPlanType


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
    help="Workspace region",
)
@click.option(
    "--cluster-policy-id",
    help="ID of cluster policy to apply to Sync job reporting cluster",
)
def create_workspace_config(
    workspace_id: str,
    instance_profile_arn: str = None,
    databricks_plan_type: str = None,
    databricks_webhook_id: str = None,
    aws_region: str = None,
    cluster_policy_id: str = None,
):
    databricks_host = click.prompt(
        "Databricks host (prefix with https://)", default=DB_CONFIG.host if DB_CONFIG else None
    )
    databricks_token = click.prompt(
        "Databricks token",
        default=DB_CONFIG.token if DB_CONFIG else None,
        hide_input=True,
        show_default=False,
    )
    sync_api_key_id = click.prompt("Sync API key ID", default=API_KEY.id if API_KEY else None)
    sync_api_key_secret = click.prompt(
        "Sync API key secret",
        default=API_KEY.secret if API_KEY else None,
        hide_input=True,
        show_default=False,
    )

    response = workspace.create_workspace_config(
        workspace_id,
        databricks_host=databricks_host,
        databricks_token=databricks_token,
        sync_api_key_id=sync_api_key_id,
        sync_api_key_secret=sync_api_key_secret,
        instance_profile_arn=instance_profile_arn,
        webhook_id=databricks_webhook_id,
        databricks_plan_type=databricks_plan_type,
        aws_region=aws_region,
        cluster_policy_id=cluster_policy_id,
    )
    config = response.result
    if config:
        click.echo(
            orjson.dumps(
                config,
                option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
            )
        )
    else:
        click.echo(str(response.error), err=True)


@workspaces.command
@click.argument("workspace-id")
def get_workspace_config(workspace_id: str):
    config_response = workspace.get_workspace_config(workspace_id)
    config = config_response.result
    if config:
        click.echo(
            orjson.dumps(
                config,
                option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
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
def update_workspace_config(
    workspace_id: str,
    instance_profile_arn: str = None,
    databricks_plan_type: str = None,
    databricks_webhook_id: str = None,
    aws_region: str = None,
    cluster_policy_id: str = None,
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

        update_config_response = workspace.update_workspace_config(
            workspace_id,
            databricks_host=databricks_host if databricks_host != OPTIONAL_DEFAULT else None,
            databricks_token=databricks_token if databricks_token != OPTIONAL_DEFAULT else None,
            sync_api_key_id=sync_api_key_id if sync_api_key_id != OPTIONAL_DEFAULT else None,
            sync_api_key_secret=sync_api_key_secret
            if sync_api_key_secret != OPTIONAL_DEFAULT
            else None,
            instance_profile_arn=instance_profile_arn,
            webhook_id=databricks_webhook_id,
            databricks_plan_type=databricks_plan_type,
            aws_region=aws_region,
            cluster_policy_id=cluster_policy_id,
        )
        config = update_config_response.result
        if config:
            click.echo(
                orjson.dumps(
                    config,
                    option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
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
            orjson.dumps(
                result,
                option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
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
