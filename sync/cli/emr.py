from io import TextIOWrapper

import click
import orjson

from sync import emr as sync_emr
from sync.api.predictions import get_prediction
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import Platform, Preference


@click.group
def emr():
    pass


@emr.command
@click.argument("job-flow", type=click.File("r"))
@click.option("-p", "--project", callback=validate_project)
@click.option("-r", "--region")
def run_job_flow(job_flow: TextIOWrapper, project: dict = None, region: str = None):
    job_flow_obj = orjson.loads(job_flow.read())

    run_response = sync_emr.run_and_record_job_flow(
        job_flow_obj, project["id"] if project else None
    )
    if prediction_id := run_response.result:
        click.echo(f"Run complete. Prediction ID: {prediction_id}")
    else:
        click.echo(str(run_response.error), err=True)


@emr.command
@click.argument("prediction-id")
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
def run_prediction(prediction_id: str, preference: Preference):
    prediction_response = get_prediction(prediction_id, preference.value)

    if prediction := prediction_response.result:
        config = prediction["solutions"][preference.value]["configuration"]

        if prediction["product_code"] == Platform.EMR.api_name:
            cluster_response = sync_emr.run_job_flow(config, prediction.get("project_id"))
            if cluster_id := cluster_response.result:
                click.echo(f"EMR cluster ID: {cluster_id}")
            else:
                click.echo(str(cluster_response.error), err=True)
        else:
            click.echo("Prediction is not for EMR", err=True)
    else:
        click.echo(str(prediction_response.error), err=True)


@emr.command
@click.argument("cluster-id")
@click.option("-p", "--project", callback=validate_project)
def create_prediction(cluster_id: str, project: str = None):
    prediction_response = sync_emr.create_prediction_for_cluster(cluster_id, project["id"])
    if prediction := prediction_response.result:
        click.echo(f"Prediction ID: {prediction}")
    else:
        click.echo(f"Failed to create prediction. {prediction_response.error}", err=True)


@emr.command
@click.argument("project", callback=validate_project)
@click.option("-r", "--run-id")
def create_project_prediction(project: dict[str, str], run_id: str = None):
    prediction_response = sync_emr.create_project_prediction(project["id"], run_id)
    if prediction := prediction_response.result:
        click.echo(f"Prediction ID: {prediction}")
    else:
        click.echo(f"Failed to create prediction. {prediction_response.error}", err=True)


@emr.command
@click.argument("cluster-id")
def get_config(cluster_id: str):
    config_response = sync_emr.get_cluster_config(cluster_id)
    if config := config_response.result:
        click.echo(
            orjson.dumps(
                config, option=orjson.OPT_INDENT_2 | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z
            )
        )
    else:
        click.echo(f"Failed to create prediction. {config_response.error}", err=True)


@emr.command
@click.argument("cluster-id")
@click.argument("project", callback=validate_project)
@click.option("-r", "--region")
def record_run(cluster_id: str, project: str, region: str = None):
    response = sync_emr.record_run(cluster_id, project["id"], region)
    if prediction_id := response.result:
        click.echo(f"Prediction ID: {prediction_id}")
    else:
        click.echo(str(response.error), err=True)
