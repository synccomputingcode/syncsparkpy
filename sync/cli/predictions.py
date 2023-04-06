import io
from pathlib import Path
from urllib.parse import urlparse

import boto3 as boto
import click
import orjson

from sync.api.predictions import (
    create_prediction,
    create_prediction_with_eventlog_bytes,
    get_prediction,
    get_predictions,
    get_products,
    get_status,
    wait_for_prediction,
)
from sync.cli.util import validate_project
from sync.config import CONFIG
from sync.models import Platform, Preference


@click.group
def predictions():
    """Sync prediction commands"""
    pass


@predictions.command
def platforms():
    """List supported platforms"""
    products_response = get_products()
    if products := products_response.result:
        click.echo(", ".join(product for product in products if product != "aws-databricks"))
    else:
        click.echo(str(products_response.error), err=True)


@predictions.command
@click.argument(
    "platform",
    type=click.Choice(platform for platform in Platform if platform is not Platform.AWS_DATABRICKS),
)
@click.option("-e", "--event-log", metavar="URL/PATH", required=True)
@click.option("-r", "--record", metavar="URL/PATH", required=True)
@click.option("--project", callback=validate_project, help="project/app ID")
@click.option(
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
@click.pass_context
def generate(
    ctx: click.Context,
    platform: Platform,
    event_log: str,
    record: str,
    project: str,
    preference: Preference,
):
    """Create and retrieve a prediction"""
    parsed_record_arg = urlparse(record)
    match parsed_record_arg.scheme:
        case "":
            with open(record) as record_fobj:
                record = orjson.loads(record_fobj.read())
        case "s3":
            s3 = boto.client("s3")
            record_io = io.BytesIO()
            s3.download_fileobj(
                parsed_record_arg.netloc, parsed_record_arg.path.lstrip("/"), record_io
            )
            record = orjson.loads(record_io.getvalue())
        case _:
            ctx.fail("Unsupported record argument")

    parsed_event_log_loc = urlparse(event_log)
    event_log_path = None
    event_log_url = None
    match parsed_event_log_loc.scheme:
        case "":
            event_log_path = Path(event_log)
        case "s3" | "http" | "https":
            event_log_url = event_log
        case _:
            ctx.fail("Unsupported event log argument")

    if event_log_url:
        response = create_prediction(platform, record, event_log_url, project["id"])
    elif event_log_path:
        with open(event_log_path, "rb") as event_log_fobj:
            response = create_prediction_with_eventlog_bytes(
                platform, record, event_log_path.name, event_log_fobj.read(), project["id"]
            )

    if prediction_id := response.result:
        click.echo(f"Prediction ID: {prediction_id}")
        click.echo("Waiting for result...")
        prediction_response = wait_for_prediction(prediction_id, preference.value)
        if prediction := prediction_response.result:
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
            click.echo(str(response.error), err=True)
    else:
        click.echo(str(response.error), err=True)


@predictions.command
@click.argument(
    "platform",
    type=click.Choice(platform for platform in Platform if platform is not Platform.AWS_DATABRICKS),
)
@click.option("-e", "--event-log", metavar="URL/PATH", required=True)
@click.option("-r", "--record", metavar="URL/PATH", required=True)
@click.option("-p", "--project", callback=validate_project, help="project/app ID")
@click.pass_context
def create(ctx: click.Context, platform: Platform, event_log: str, record: str, project: str):
    """Create a prediction"""
    parsed_record_arg = urlparse(record)
    match parsed_record_arg.scheme:
        case "":
            with open(record) as record_fobj:
                record = orjson.loads(record_fobj.read())
        case "s3":
            s3 = boto.client("s3")
            record_io = io.BytesIO()
            s3.download_fileobj(
                parsed_record_arg.netloc, parsed_record_arg.path.lstrip("/"), record_io
            )
            record = orjson.loads(record_io.getvalue())
        case _:
            ctx.fail("Unsupported record argument")

    parsed_event_log_loc = urlparse(event_log)
    event_log_path = None
    event_log_url = None
    match parsed_event_log_loc.scheme:
        case "":
            event_log_path = Path(event_log)
        case "s3" | "http" | "https":
            event_log_url = event_log
        case _:
            ctx.fail("Unsupported event log argument")

    if event_log_url:
        response = create_prediction(platform, record, event_log_url, project["id"])
    elif event_log_path:
        with open(event_log_path, "rb") as event_log_fobj:
            response = create_prediction_with_eventlog_bytes(
                platform, record, event_log_path.name, event_log_fobj.read(), project["id"]
            )

    if prediction_id := response.result:
        click.echo(f"Prediction ID: {prediction_id}")
    else:
        click.echo(str(response.error), err=True)


@predictions.command
@click.argument("prediction-id")
def status(prediction_id: str):
    """Get the status of a prediction"""
    click.echo(get_status(prediction_id).result)


@predictions.command
@click.argument("prediction-id")
@click.option(
    "-p",
    "--preference",
    type=click.Choice(Preference),
    default=CONFIG.default_prediction_preference,
)
def get(prediction_id: str, preference: Preference):
    """Retrieve a prediction"""
    response = get_prediction(prediction_id, preference.value)
    click.echo(
        orjson.dumps(
            response.result,
            option=orjson.OPT_INDENT_2
            | orjson.OPT_UTC_Z
            | orjson.OPT_NAIVE_UTC
            | orjson.OPT_OMIT_MICROSECONDS,
        )
    )


@predictions.command
@click.option("--platform", type=click.Choice(Platform))
@click.option("--project", callback=validate_project, help="project/app ID")
def list(platform: Platform, project: dict = None):
    """List predictions"""
    response = get_predictions(
        product=platform.value if platform else None, project_id=project["id"]
    )
    if predictions := response.result:
        click.echo_via_pager(
            f"{p['created_at']} {p['prediction_id']} ({p.get('project_id', 'not part of a project'):^36}): {p['application_name']}\n"
            for p in predictions
        )
    else:
        click.echo(str(response.error), err=True)
