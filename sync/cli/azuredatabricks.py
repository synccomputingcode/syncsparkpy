import click

from sync.cli._databricks import (
    access_report,
    create_prediction,
    create_submission,
    create_workspace_config,
    delete_workspace_config,
    get_cluster_report,
    get_workspace_config,
    list_workspace_configs,
    monitor_cluster,
    run_job,
    run_prediction,
    update_workspace_config,
)
from sync.models import Platform


@click.group
@click.pass_context
def azure_databricks(ctx: click.Context):
    """Databricks on AWS commands"""
    ctx.obj = Platform.AZURE_DATABRICKS


azure_databricks.add_command(access_report)
azure_databricks.add_command(run_prediction)
azure_databricks.add_command(run_job)
azure_databricks.add_command(create_prediction)
azure_databricks.add_command(create_submission)
azure_databricks.add_command(get_cluster_report)
azure_databricks.add_command(monitor_cluster)
azure_databricks.add_command(create_workspace_config)
azure_databricks.add_command(get_workspace_config)
azure_databricks.add_command(list_workspace_configs)
azure_databricks.add_command(update_workspace_config)
azure_databricks.add_command(delete_workspace_config)
