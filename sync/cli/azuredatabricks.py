import click

from sync.cli._databricks import (
    access_report,
    create_prediction,
    get_cluster_report,
    monitor_cluster,
    run_job,
    run_prediction,
)
from sync.models import Platform


@click.group
@click.pass_context
def azure_databricks(ctx: click.Context):
    """Databricks on AWS commands"""
    ctx.obj = Platform.AZURE_DATABRICKS


azure_databricks.command(access_report)
azure_databricks.command(run_prediction)
azure_databricks.command(run_job)
azure_databricks.command(create_prediction)
azure_databricks.command(get_cluster_report)
azure_databricks.command(monitor_cluster)
