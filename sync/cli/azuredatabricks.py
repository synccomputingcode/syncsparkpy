import click

from sync.cli._databricks import (
    access_report,
    create_prediction,
    create_recommendation,
    create_submission,
    get_cluster_report,
    get_recommendation,
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


azure_databricks.add_command(access_report)
azure_databricks.add_command(run_prediction)
azure_databricks.add_command(run_job)
azure_databricks.add_command(create_prediction)
azure_databricks.add_command(create_submission)
azure_databricks.add_command(create_recommendation)
azure_databricks.add_command(get_recommendation)
azure_databricks.add_command(get_cluster_report)
azure_databricks.add_command(monitor_cluster)
