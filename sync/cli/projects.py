import click
import orjson

from ..api.projects import get_project, get_projects


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
@click.argument("project-id")
def get(project_id: str):
    response = get_project(project_id)
    if project := response.result:
        click.echo(
            orjson.dumps(
                project,
                option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
            )
        )
    else:
        click.echo(f"{response.error.code}: {response.error.message}", err=True)
