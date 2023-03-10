import click

from sync import databricks as sync_databricks
from sync.cli import validate_project
from sync.config import CONFIG
from sync.models import Preference


@click.group
def databricks():
    pass


@databricks.command
@click.argument("job-id")
@click.argument("prediction-id")
@click.option(
    "-p",
    "--preference",
    type=click.Choice([p.value for p in Preference]),
    default=CONFIG.default_prediction_preference,
)
def run_prediction(job_id: str, prediction_id: str, preference: str = None):
    run = sync_databricks.run_prediction(job_id, prediction_id, preference)
    if run_id := run.result:
        click.echo(f"Run ID: {run_id}")
    else:
        click.echo(run.error, err=True)


@databricks.command
@click.argument("job-id")
@click.option(
    "--plan", type=click.Choice(["Standard", "Premium", "Enterprise"]), default="Standard"
)
@click.option(
    "--compute",
    type=click.Choice(["All-Purpose Compute", "Jobs Compute", "Jobs Light Compute"]),
    default="Jobs Compute",
)
@click.option("-p", "--project", callback=validate_project)
def run_job(job_id: str, plan: str, compute: str, project: dict = None):
    run_response = sync_databricks.run_and_record_job(
        job_id, plan, compute, project["id"] if project else None
    )
    if prediction_id := run_response.result:
        click.echo(f"Predction ID: {prediction_id}")
    else:
        click.echo(run_response.error, err=True)


@databricks.command
@click.argument("run-id")
@click.option(
    "--plan", type=click.Choice(["Standard", "Premium", "Enterprise"]), default="Standard"
)
@click.option(
    "--compute",
    type=click.Choice(["All-Purpose Compute", "Jobs Compute", "Jobs Light Compute"]),
    default="Jobs Compute",
)
@click.option("-p", "--project-id")
def create_prediction(run_id: str, plan: str, compute: str, project_id: str = None):
    prediction_response = sync_databricks.create_prediction_for_run(
        run_id, plan, compute, project_id
    )
    if prediction := prediction_response.result:
        click.echo(f"Prediction ID: {prediction}")
    else:
        click.echo(f"Failed to create prediction. {prediction_response.error}", err=True)
