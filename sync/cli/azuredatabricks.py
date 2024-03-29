import click

from sync.cli._databricks import (
    access_report,
    apply_recommendation,
    create_recommendation,
    create_submission,
    get_cluster_report,
    get_recommendation,
    get_submission,
    monitor_cluster,
)
from sync.models import Platform


@click.group
@click.pass_context
def azure_databricks(ctx: click.Context):
    """Databricks on AWS commands"""
    ctx.obj = Platform.AZURE_DATABRICKS


azure_databricks.add_command(access_report)
azure_databricks.add_command(create_submission)
azure_databricks.add_command(create_recommendation)
azure_databricks.add_command(get_recommendation)
azure_databricks.add_command(get_submission)
azure_databricks.add_command(apply_recommendation)
azure_databricks.add_command(get_cluster_report)
azure_databricks.add_command(monitor_cluster)
