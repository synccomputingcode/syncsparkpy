import click

from sync.cli._databricks import (
    access_report,
    create_prediction,
    create_submission,
    get_cluster_report,
    monitor_cluster,
    run_job,
    run_prediction,
)
from sync.models import Platform


@click.group
@click.pass_context
def aws_databricks(ctx: click.Context):
    """Databricks on AWS commands"""
    ctx.obj = Platform.AWS_DATABRICKS


aws_databricks.add_command(access_report)
aws_databricks.add_command(run_prediction)
aws_databricks.add_command(run_job)
aws_databricks.add_command(create_prediction)
aws_databricks.add_command(create_submission)
aws_databricks.add_command(get_cluster_report)
aws_databricks.add_command(monitor_cluster)
