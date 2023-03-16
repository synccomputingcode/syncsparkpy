import click
import orjson

from sync.api.predictions import get_predictions
from sync.api.projects import create_project, get_project, get_projects, update_project
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import Preference


@click.group
def projects():
    pass


@projects.command
def list():
    response = get_projects()
    if projects := response.result:
        click.echo_via_pager(f"{p['updated_at']} {p['id']}: {p['app_id']}\n" for p in projects)
    else:
        click.echo(f"{response.error.code}: {response.error.message}", err=True)


@projects.command
@click.argument("project", callback=validate_project)
def get(project: dict):
    response = get_project(project["id"])
    if project := response.result:
        click.echo(
            orjson.dumps(
                project,
                option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
            )
        )
    else:
        click.echo(f"{response.error.code}: {response.error.message}", err=True)


@projects.command
@click.argument("app-id")
@click.option("-d", "--description")
@click.option("-l", "--location", help="S3 URL under which to store event logs and configuration")
@click.option(
    "-p",
    "--preference",
    type=click.Choice([p.value for p in Preference]),
    default=CONFIG.default_prediction_preference,
)
def create(app_id: str, description: str = None, location: str = None, preference: str = None):
    response = create_project(app_id, description, location, preference)
    if project := response.result:
        click.echo(f"Project ID: {project['id']}")
    else:
        click.echo(str(response.error), err=True)


@projects.command
@click.argument("project-id")
@click.option("-d", "--description")
@click.option("-l", "--location", help="S3 URL under which to store event logs and configuration")
@click.option(
    "-p",
    "--preference",
    type=click.Choice([p.value for p in Preference]),
    default=CONFIG.default_prediction_preference,
)
def update(project_id: str, description: str = None, location: str = None, preference: str = None):
    response = update_project(project_id, description, location, preference)
    if response.result:
        click.echo("Project updated")
    else:
        click.echo(str(response.error), err=True)


@projects.command("get-prediction")
@click.argument("project", callback=validate_project)
@click.option(
    "-p",
    "--preference",
    type=click.Choice([p.value for p in Preference]),
    default=CONFIG.default_prediction_preference,
)
def get_latest_prediction(project: dict, preference: Preference):
    predictions_response = get_predictions(project_id=project["id"])
    if predictions_response.error:
        click.echo(str(predictions_response.error), err=True)
    elif predictions := predictions_response.result:
        click.echo(
            orjson.dumps(
                predictions[0]["solutions"][preference],
                option=orjson.OPT_INDENT_2
                | orjson.OPT_UTC_Z
                | orjson.OPT_NAIVE_UTC
                | orjson.OPT_OMIT_MICROSECONDS,
            )
        )
    else:
        click.echo(f"No predictions found for project {project['id']}", err=True)
