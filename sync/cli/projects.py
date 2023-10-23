import click
import orjson

from sync.api.projects import (
    create_project,
    delete_project,
    get_prediction,
    get_project,
    get_projects,
    update_project,
)
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import Preference


@click.group
def projects():
    """Sync project commands"""
    pass


@projects.command
def list():
    """List projects"""
    response = get_projects()
    projects = response.result
    if projects:
        click.echo_via_pager(f"{p['updated_at']} {p['id']}: {p['name']}\n" for p in projects)
    else:
        click.echo(str(response.error), err=True)


@projects.command
@click.argument("project", callback=validate_project)
def get(project: dict):
    """Get a project

    PROJECT is either a project ID or application name"""
    response = get_project(project["id"])
    project = response.result
    if project:
        click.echo(
            orjson.dumps(
                project,
                option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
            )
        )
    else:
        click.echo(str(response.error), err=True)


@projects.command
@click.argument("name")
@click.argument("product_code")
@click.option("-d", "--description")
@click.option("-j", "--job-id", help="Databricks job ID")
@click.option("-l", "--location", help="S3 URL under which to store event logs and configuration")
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
@click.option(
    "-i", "--app-id", help="External identifier often based on the project's target application"
)
def create(
    name: str,
    product_code: str,
    description: str = None,
    job_id: str = None,
    location: str = None,
    preference: Preference = None,
    app_id: str = None,
):
    """Create a project for a Spark application that runs on the platform identified by PRODUCT_CODE.
    Run `sync-cli products` to see a list of available product codes.
    """
    response = create_project(
        name,
        product_code,
        description=description,
        job_id=job_id,
        cluster_log_dest=location,
        prediction_preference=preference,
        app_id=app_id,
    )
    project = response.result
    if project:
        click.echo(f"Project ID: {project['id']}")
    else:
        click.echo(str(response.error), err=True)


@projects.command
@click.argument("project-id")
@click.option("-d", "--description")
@click.option("-l", "--location", help="S3 URL under which to store event logs and configuration")
@click.option(
    "-i", "--app-id", help="External identifier often based on the project's target application"
)
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
def update(
    project_id: str,
    description: str = None,
    location: str = None,
    app_id: str = None,
    preference: Preference = None,
):
    """Update a project"""
    response = update_project(project_id, description, location, app_id, preference)
    if response.result:
        click.echo("Project updated")
    else:
        click.echo(str(response.error), err=True)


@projects.command
@click.argument("project", callback=validate_project)
def delete(project: dict):
    """Delete a project

    PROJECT is either a project ID or application name"""
    response = delete_project(project["id"])
    if response.result:
        click.echo(response.result)
    else:
        click.echo(str(response.error), err=True)


@projects.command("get-prediction")
@click.argument("project", callback=validate_project)
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
)
def get_latest_prediction(project: dict, preference: Preference):
    """Get the latest prediction in a project"""
    prediction_response = get_prediction(project["id"], preference)
    prediction = prediction_response.result
    if prediction:
        click.echo(
            orjson.dumps(
                prediction,
                option=orjson.OPT_INDENT_2
                | orjson.OPT_UTC_Z
                | orjson.OPT_NAIVE_UTC
                | orjson.OPT_OMIT_MICROSECONDS,
            )
        )
    else:
        click.echo(str(prediction_response.error), err=True)
