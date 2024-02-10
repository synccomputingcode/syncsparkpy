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
def aws_databricks(ctx: click.Context):
    """Databricks on AWS commands"""
    ctx.obj = Platform.AWS_DATABRICKS


aws_databricks.add_command(access_report)
aws_databricks.add_command(create_submission)
aws_databricks.add_command(create_recommendation)
aws_databricks.add_command(get_recommendation)
aws_databricks.add_command(get_submission)
aws_databricks.add_command(apply_recommendation)
aws_databricks.add_command(get_cluster_report)
aws_databricks.add_command(monitor_cluster)
