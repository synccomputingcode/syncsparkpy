import json

import click

from sync.api.projects import (
    create_project,
    delete_project,
    get_project,
    get_projects,
    get_submissions,
    reset_project,
    update_project,
)
from sync.cli.util import validate_project
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
@click.option("--auto-apply-recs", type=bool, help="Automatically apply project recommendations")
def update(
    project_id: str,
    description: str = None,
    location: str = None,
    app_id: str = None,
    cluster_path: str = None,
    workspace_id: str = None,
    auto_apply_recs: bool = None,
    job_id: str = None,
    optimize_instance_size: bool = None,
    optimize_worker_instance: bool = None,
):
    """Update a project"""
    response = update_project(
        project_id,
        description=description,
        cluster_log_url=location,
        app_id=app_id,
        cluster_path=cluster_path,
        workspace_id=workspace_id,
        auto_apply_recs=auto_apply_recs,
        job_id=job_id,
        optimize_instance_size=optimize_instance_size,
        optimize_worker_instance=optimize_worker_instance,
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


@projects.command
@click.argument("project", callback=validate_project)
@click.option(
    "--success-only",
    is_flag=True,
    default=False,
    help="Only show the most recent successful submission",
)
def get_latest_submission_config(project: dict, success_only: bool = False):
    """
    Get the latest submission configuration for a project.
    """

    try:
        submissions = get_submissions(project["id"]).result
    except AttributeError as e:
        click.echo(f"Failed to retrieve submissions. {e}", err=True)
        return

    if not submissions:
        click.echo("No submissions found.", err=True)
        return

    if success_only:
        latest_successful_submission = next(
            (submission for submission in submissions if submission["state"] == "SUCCESS"), None
        )

        if not latest_successful_submission:
            click.echo("No successful submissions found.", err=True)
            return

        submission_to_show = latest_successful_submission
    else:
        submission_to_show = submissions[0]
    click.echo(
        json.dumps(
            submission_to_show.get("configuration", {}),
            indent=2,
            cls=DateTimeEncoderNaiveUTCDropMicroseconds,
        )
    )
