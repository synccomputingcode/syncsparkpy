import click

from sync.cli._databricks import access_report, run_prediction
from sync.models import Platform


@click.group
@click.pass_context
def aws_databricks(ctx: click.Context):
    """Databricks on AWS commands"""
    ctx.platform = Platform.AWS_DATABRICKS


aws_databricks.command(access_report)
aws_databricks.command(run_prediction)
