from typing import Tuple

import click
import orjson

from sync import awsdatabricks
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import DatabricksComputeType, DatabricksPlanType, Preference


@click.group
def aws_databricks():
    """Databricks on AWS commands"""


@aws_databricks.command
@click.option("--log-url")
def access_report(log_url: str = None):
    """Get access report"""
    click.echo(awsdatabricks.get_access_report(log_url))


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
    run_id = run.result
    if run_id:
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
    prediction_id = run_response.result
    if prediction_id:
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
@click.option(
    "--allow-incomplete",
    is_flag=True,
    default=False,
    help="Force creation of a prediction even with incomplete cluster data.",
)
@click.option(
    "--exclude-task", help="Don't consider task when finding the cluster of a run", multiple=True
)
def create_prediction(
    run_id: str,
    plan: DatabricksPlanType,
    compute: DatabricksComputeType,
    project: dict = None,
    allow_incomplete: bool = False,
    exclude_task: Tuple[str, ...] = None,
):
    """Create a prediction for a job run"""
    prediction_response = awsdatabricks.create_prediction_for_run(
        run_id, plan, compute, project["id"], allow_incomplete, exclude_task
    )
    prediction = prediction_response.result
    if prediction:
        click.echo(f"Prediction ID: {prediction}")
    else:
        click.echo(f"Failed to create prediction. {prediction_response.error}", err=True)


@aws_databricks.command
@click.argument("run-id")
@click.option("--plan", type=click.Choice(DatabricksPlanType), default=DatabricksPlanType.STANDARD)
@click.option(
    "--compute",
    type=click.Choice(DatabricksComputeType),
    default=DatabricksComputeType.JOBS_COMPUTE,
)
@click.option(
    "--allow-incomplete",
    is_flag=True,
    default=False,
    help="Force creation of a cluster report even if some data is missing.",
)
def get_cluster_report(
    run_id: str,
    plan: DatabricksPlanType,
    compute: DatabricksComputeType,
    allow_incomplete: bool = False,
):
    """Get a cluster report"""
    config_response = awsdatabricks.get_cluster_report(run_id, plan, compute, allow_incomplete)
    config = config_response.result
    if config:
        click.echo(
            orjson.dumps(
                config.dict(exclude_none=True),
                option=orjson.OPT_INDENT_2 | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z,
            )
        )
    else:
        click.echo(f"Failed to create cluster report. {config_response.error}", err=True)


@aws_databricks.command
@click.argument("cluster-id")
def monitor_cluster(cluster_id: str):
    awsdatabricks.monitor_cluster(cluster_id)
