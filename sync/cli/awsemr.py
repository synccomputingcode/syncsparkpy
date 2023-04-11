from io import TextIOWrapper

import click
import orjson

from sync import awsemr
from sync.api.predictions import get_prediction
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import Platform, Preference


@click.group
def aws_emr():
    """EMR commands"""
    pass


@aws_emr.command
@click.argument("job-flow", type=click.File("r"))
@click.option("-p", "--project", callback=validate_project)
@click.option("-r", "--region")
def run_job_flow(job_flow: TextIOWrapper, project: dict = None, region: str = None):
    """Run a job flow

    JOB_FLOW is a file containing the RunJobFlow request object"""
    job_flow_obj = orjson.loads(job_flow.read())

    run_response = awsemr.run_and_record_job_flow(
        job_flow_obj, project["id"] if project else None, region
    )
    if prediction_id := run_response.result:
        click.echo(f"Run complete. Prediction ID: {prediction_id}")
    else:
        click.echo(str(run_response.error), err=True)


@aws_emr.command
@click.argument("prediction-id")
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
@click.option("-r", "--region")
def run_prediction(prediction_id: str, preference: Preference, region: str = None):
    """Execute a prediction"""
    prediction_response = get_prediction(prediction_id, preference.value)

    if prediction := prediction_response.result:
        config = prediction["solutions"][preference.value]["configuration"]

        if prediction["product_code"] == Platform.AWS_EMR:
            cluster_response = awsemr.run_job_flow(config, prediction.get("project_id"), region)
            if cluster_id := cluster_response.result:
                click.echo(f"EMR cluster ID: {cluster_id}")
            else:
                click.echo(str(cluster_response.error), err=True)
        else:
            click.echo("Prediction is not for EMR", err=True)
    else:
        click.echo(str(prediction_response.error), err=True)


@aws_emr.command
@click.argument("cluster-id")
@click.option("-r", "--region")
def create_prediction(cluster_id: str, region: str = None):
    """Create prediction for a cluster"""
    prediction_response = awsemr.create_prediction_for_cluster(cluster_id, region)
    if prediction := prediction_response.result:
        click.echo(f"Prediction ID: {prediction}")
    else:
        click.echo(f"Failed to create prediction. {prediction_response.error}", err=True)


@aws_emr.command
@click.argument("project", callback=validate_project)
@click.option("-r", "--run-id")
@click.option("-r", "--region")
def create_project_prediction(project: dict[str, str], run_id: str = None, region: str = None):
    """Create prediction for the latest project cluster or one specified by --run-id"""
    prediction_response = awsemr.create_project_prediction(project["id"], run_id, region)
    if prediction := prediction_response.result:
        click.echo(f"Prediction ID: {prediction}")
    else:
        click.echo(f"Failed to create prediction. {prediction_response.error}", err=True)


@aws_emr.command
@click.argument("cluster-id")
@click.option("-r", "--region")
def get_cluster_report(cluster_id: str, region: str = None):
    """Get a cluster report"""
    config_response = awsemr.get_cluster_report(cluster_id, region)
    if config := config_response.result:
        click.echo(
            orjson.dumps(
                config, option=orjson.OPT_INDENT_2 | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z
            )
        )
    else:
        click.echo(f"Failed to create prediction. {config_response.error}", err=True)


@aws_emr.command
@click.argument("cluster-id")
@click.argument("project", callback=validate_project)
@click.option("-r", "--region")
def record_run(cluster_id: str, project: str, region: str = None):
    """Record a project run"""
    response = awsemr.record_run(cluster_id, project["id"], region)
    if prediction_id := response.result:
        click.echo(f"Prediction ID: {prediction_id}")
    else:
        click.echo(str(response.error), err=True)
