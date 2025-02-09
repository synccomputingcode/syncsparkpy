import json
from typing import Optional, Tuple

import click

from sync.api.projects import (
    create_project_recommendation,
    get_project_recommendation,
    get_project_submission,
)
from sync.cli.util import validate_project
from sync.models import DatabricksComputeType, DatabricksPlanType, Platform
from sync.utils.json import DateTimeEncoderNaiveUTC

pass_platform = click.make_pass_decorator(Platform)

OPTIONAL_DEFAULT = "none"


@click.command
@click.option("--log-url")
@pass_platform
def access_report(platform: Platform, log_url: Optional[str] = None):
    """Get access report"""
    if platform is Platform.AWS_DATABRICKS:
        import sync.awsdatabricks as databricks
    elif platform is Platform.AZURE_DATABRICKS:
        import sync.azuredatabricks as databricks

    click.echo(databricks.get_access_report(log_url))


@click.command
@click.argument("run-id")
@click.argument("project", callback=validate_project)
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
    help="Force creation of a submission even with incomplete cluster data. Some features may not be available. To ensure a complete cluster report see https://docs.synccomputing.com/sync-gradient/integrating-with-gradient/databricks-workflows.",
)
@click.option(
    "--exclude-task", help="Don't consider task when finding the cluster of a run", multiple=True
)
@pass_platform
def create_submission(
    platform: Platform,
    run_id: str,
    plan: DatabricksPlanType,
    compute: DatabricksComputeType,
    project: dict,
    allow_incomplete: bool = False,
    exclude_task: Optional[Tuple[str, ...]] = None,
):
    """Create a submission for a job run"""
    if platform is Platform.AWS_DATABRICKS:
        import sync.awsdatabricks as databricks
    elif platform is Platform.AZURE_DATABRICKS:
        import sync.azuredatabricks as databricks

    submission_response = databricks.create_submission_for_run(
        run_id, plan, compute, project["id"], allow_incomplete, exclude_task
    )
    submission = submission_response.result
    if submission:
        click.echo(f"Submission ID: {submission}")
    else:
        click.echo(f"Failed to submit data. {submission_response.error}", err=True)


@click.command
@click.argument("project", callback=validate_project)
def create_recommendation(project: dict):
    rec_response = create_project_recommendation(project["id"])
    recommendation_id = rec_response.result
    if recommendation_id:
        click.echo(f"Recommendation ID: {recommendation_id}")
    else:
        click.echo(f"Failed to create recommendation. {rec_response.error}", err=True)


@click.command
@click.argument("project", callback=validate_project)
@click.argument("recommendation-id")
def get_recommendation(project: dict, recommendation_id: str):
    """Get a project recommendation"""
    rec_response = get_project_recommendation(project["id"], recommendation_id)
    recommendation = rec_response.result
    if recommendation:
        if recommendation["state"] == "FAILURE":
            click.echo("Recommendation generation failed.", err=True)
        else:
            click.echo(
                json.dumps(
                    recommendation,
                    indent=2,
                    cls=DateTimeEncoderNaiveUTC,
                )
            )
    else:
        click.echo(f"Failed to get recommendation. {rec_response.error}", err=True)


@click.command
@click.argument("project", callback=validate_project)
@click.argument("submission-id")
def get_submission(project: dict, submission_id: str):
    """Get a project submission"""
    sub_response = get_project_submission(project["id"], submission_id)
    submission = sub_response.result
    if submission:
        if submission["state"] == "FAILURE":
            click.echo("Submission generation failed.", err=True)
        else:
            click.echo(
                json.dumps(
                    submission,
                    indent=2,
                    cls=DateTimeEncoderNaiveUTC,
                )
            )
    else:
        click.echo(f"Failed to get submission. {sub_response.error}", err=True)


@click.command
@click.argument("run-id")
@click.option("--plan", type=click.Choice(DatabricksPlanType), default=DatabricksPlanType.STANDARD)
@click.option(
    "--compute",
    type=click.Choice(DatabricksComputeType),
    default=DatabricksComputeType.JOBS_COMPUTE,
)
@click.option(
    "--project",
    callback=validate_project,
    help="The project ID for which to generate a cluster report, if any. This is most relevant to runs that may utilize multiple clusters.",
)
@click.option(
    "--allow-incomplete",
    is_flag=True,
    default=False,
    help="Force creation of a cluster report even if some data is missing. Some features require a complete report. To ensure a complete report see https://docs.synccomputing.com/sync-gradient/integrating-with-gradient/databricks-workflows.",
)
@click.option(
    "--exclude-task", help="Don't consider task when finding the cluster of a run", multiple=True
)
@pass_platform
def get_cluster_report(
    platform: Platform,
    run_id: str,
    plan: DatabricksPlanType,
    compute: DatabricksComputeType,
    project: Optional[dict] = None,
    allow_incomplete: bool = False,
    exclude_task: Optional[Tuple[str, ...]] = None,
):
    """Get a cluster report"""
    if platform is Platform.AWS_DATABRICKS:
        import sync.awsdatabricks as databricks
    elif platform is Platform.AZURE_DATABRICKS:
        import sync.azuredatabricks as databricks

    config_response = databricks.get_cluster_report(
        run_id, plan, compute, project["id"], allow_incomplete, exclude_task
    )

    config = config_response.result
    if config:
        click.echo(
            json.dumps(
                config.dict(exclude_none=True),
                indent=2,
                cls=DateTimeEncoderNaiveUTC,
            )
        )
    else:
        click.echo(f"Failed to create cluster report. {config_response.error}", err=True)


@click.command
@click.argument("job-id")
@click.argument("project-id")
@click.argument("recommendation-id")
@pass_platform
def apply_recommendation(
    platform: Platform,
    job_id: str,
    project_id: str,
    recommendation_id: Optional[str] = None,
):
    """Apply a project recommendation to a job"""
    if platform is Platform.AWS_DATABRICKS:
        import sync.awsdatabricks as databricks
    elif platform is Platform.AZURE_DATABRICKS:
        import sync.azuredatabricks as databricks

    response = databricks.apply_project_recommendation(job_id, project_id, recommendation_id)
    recommendation_id = response.result
    if recommendation_id:
        click.echo(f"Applied recommendation {recommendation_id} to job {job_id}")
    else:
        click.echo(f"Failed to apply recommendation. {response.error}", err=True)


@click.command
@click.argument("cluster-id")
@pass_platform
def monitor_cluster(platform: Platform, cluster_id: str):
    if platform is Platform.AWS_DATABRICKS:
        import sync.awsdatabricks as databricks
    elif platform is Platform.AZURE_DATABRICKS:
        import sync.azuredatabricks as databricks

    databricks.monitor_cluster(cluster_id)
