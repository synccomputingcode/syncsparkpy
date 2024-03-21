import json

import click

from sync.api.projects import (
    create_project,
    delete_project,
    get_prediction,
    get_project,
    get_projects,
    reset_project,
    update_project,
)
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import Preference
from sync.utils.json import DateTimeEncoderNaiveUTCDropMicroseconds


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
        click.echo(json.dumps(project, indent=2, cls=DateTimeEncoderNaiveUTCDropMicroseconds))
    else:
        click.echo(str(response.error), err=True)


@projects.command
@click.argument("name")
@click.argument("product_code")
@click.option("-d", "--description")
@click.option("-j", "--job-id", help="Databricks job ID")
@click.option(
    "-c",
    "--cluster-path",
    help="Path to cluster definition in job object, e.g. 'job_clusters/Job_cluster'",
)
@click.option("-w", "--workspace-id", help="Databricks workspace ID")
@click.option("-l", "--location", help="S3 URL under which to store event logs and configuration")
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
@click.option(
    "--auto-apply-recs",
    is_flag=True,
    default=False,
    help="Automatically apply project recommendations",
)
@click.option(
    "-i", "--app-id", help="External identifier often based on the project's target application"
)
def create(
    name: str,
    product_code: str,
    auto_apply_recs: bool,
    description: str = None,
    job_id: str = None,
    cluster_path: str = None,
    workspace_id: str = None,
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
        cluster_path=cluster_path,
        workspace_id=workspace_id,
        cluster_log_url=location,
        prediction_preference=preference,
        auto_apply_recs=auto_apply_recs,
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
    "-c",
    "--cluster-path",
    help="Path to cluster definition in job object, e.g. 'job_clusters/Job_cluster'",
)
@click.option("-w", "--workspace-id", help="Databricks workspace ID")
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
@click.option("--auto-apply-recs", type=bool, help="Automatically apply project recommendations")
def update(
    project_id: str,
    description: str = None,
    location: str = None,
    app_id: str = None,
    cluster_path: str = None,
    workspace_id: str = None,
    preference: Preference = None,
    auto_apply_recs: bool = None,
    job_id: str = None,
    optimize_instance_size: bool = None,
):
    """Update a project"""
    response = update_project(
        project_id,
        description=description,
        cluster_log_url=location,
        app_id=app_id,
        cluster_path=cluster_path,
        workspace_id=workspace_id,
        prediction_preference=preference,
        auto_apply_recs=auto_apply_recs,
        job_id=job_id,
        optimize_instance_size=optimize_instance_size,
    )
    if response.result:
        click.echo("Project updated")
    else:
        click.echo(str(response.error), err=True)


@projects.command
@click.argument("project", callback=validate_project)
def reset(project: dict):
    """Reset a project

    PROJECT is either a project ID or application name"""
    response = reset_project(project["id"])
    if response.result:
        click.echo("Project reset")
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
        click.echo(json.dumps(prediction, indent=2, cls=DateTimeEncoderNaiveUTCDropMicroseconds))
    else:
        click.echo(str(prediction_response.error), err=True)
