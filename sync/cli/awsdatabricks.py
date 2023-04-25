import click
from orjson import orjson

from sync import awsdatabricks
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import DatabricksComputeType, DatabricksPlanType, Preference


@click.group
def aws_databricks():
    """Databricks on AWS commands"""
    pass


@aws_databricks.command
@click.argument("job-id")
@click.argument("prediction-id")
@click.option(
    "-p",
    "--preference",
    type=click.Choice([p.value for p in Preference]),
    default=CONFIG.default_prediction_preference,
)
def run_prediction(job_id: str, prediction_id: str, preference: str = None):
    """Apply a prediction to a job and run it"""
    run = awsdatabricks.run_prediction(job_id, prediction_id, preference)
    if run_id := run.result:
        click.echo(f"Run ID: {run_id}")
    else:
        click.echo(str(run.error), err=True)


@aws_databricks.command
@click.argument("job-id")
@click.option("--plan", type=click.Choice(DatabricksPlanType), default=DatabricksPlanType.STANDARD)
@click.option(
    "--compute",
    type=click.Choice(DatabricksComputeType),
    default=DatabricksComputeType.JOBS_COMPUTE,
)
@click.option("--project", callback=validate_project)
def run_job(
    job_id: str, plan: DatabricksPlanType, compute: DatabricksComputeType, project: dict = None
):
    """Run a job, wait for it to complete then create a prediction"""
    run_response = awsdatabricks.run_and_record_job(job_id, plan, compute, project["id"])
    if prediction_id := run_response.result:
        click.echo(f"Prediction ID: {prediction_id}")
    else:
        click.echo(str(run_response.error), err=True)


@aws_databricks.command
@click.argument("run-id")
@click.option("--plan", type=click.Choice(DatabricksPlanType), default=DatabricksPlanType.STANDARD)
@click.option(
    "--compute",
    type=click.Choice(DatabricksComputeType),
    default=DatabricksComputeType.JOBS_COMPUTE,
)
@click.option("--project", callback=validate_project)
def create_prediction(
    run_id: str, plan: DatabricksPlanType, compute: DatabricksComputeType, project: dict = None
):
    """Create a prediction for a job run"""
    prediction_response = awsdatabricks.create_prediction_for_run(
        run_id, plan, compute, project["id"]
    )
    if prediction := prediction_response.result:
        click.echo(f"Prediction ID: {prediction}")
    else:
        click.echo(f"Failed to create prediction. {prediction_response.error}", err=True)


@aws_databricks.command
@click.argument("cluster-id")
@click.option("--plan", type=click.Choice(DatabricksPlanType), default=DatabricksPlanType.STANDARD)
@click.option(
    "--compute",
    type=click.Choice(DatabricksComputeType),
    default=DatabricksComputeType.JOBS_COMPUTE,
)
def get_cluster_report(cluster_id: str, plan: DatabricksPlanType, compute: DatabricksComputeType):
    """Get a cluster report"""
    config_response = awsdatabricks.get_cluster_report(cluster_id, plan, compute)
    if config := config_response.result:
        click.echo(
            orjson.dumps(
                config.dict(exclude_none=True),
                option=orjson.OPT_INDENT_2 | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z,
            )
        )
    else:
        click.echo(f"Failed to create cluster report. {config_response.error}", err=True)
