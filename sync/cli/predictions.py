import click
import orjson

from .. import databricks, emr
from ..api.predictions import get_prediction, get_predictions, get_status
from ..config import CONFIG
from ..models import Platform, Preference


@click.group
def predictions():
    pass


@predictions.command
@click.option("-c", "--cluster-id")
def submit(cluster_id: str):
    click.echo(emr.initiate_prediction_with_cluster_id(cluster_id).result)


@predictions.command
@click.argument("prediction-id")
def status(prediction_id: str):
    click.echo(get_status(prediction_id).result)


@predictions.command
@click.argument("prediction-id")
@click.option("-p", "--preference", type=click.Choice([p.value for p in Preference]))
def get(prediction_id: str, preference: str = None):
    response = get_prediction(prediction_id, preference)
    click.echo(
        orjson.dumps(
            response.result,
            option=orjson.OPT_INDENT_2 | orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS,
        )
    )


@predictions.command
@click.argument("prediction-id")
@click.option("-p", "--preference", type=click.Choice([p.value for p in Preference]))
def apply(prediction_id: str, preference: str = None):
    response = get_prediction(prediction_id, preference)

    if result := response.result:
        config = result["solutions"][preference or CONFIG.default_prediction_preference][
            "configuration"
        ]

        match Platform.from_api_name(result["product_code"]):
            case Platform.EMR:
                cluster_response = emr.run_job_flow(config, result.get("project_id"))
                if result := cluster_response.result:
                    click.echo(f"EMR cluster ID: {result}")
                else:
                    click.echo(f"{response.error.code}: {response.error.message}", err=True)
            case Platform.DATABRICKS:
                cluster_response = databricks.create_cluster(config)
                if result := cluster_response.result:
                    click.echo(f"Databricks cluster ID: {result}")
                else:
                    click.echo(f"{response.error.code}: {response.error.message}", err=True)
    else:
        click.echo(f"{response.error.code}: {response.error.message}", err=True)


@predictions.command
@click.option("--platform", type=click.Choice(Platform))
@click.option("--project", "project_id", metavar="PROJECT_ID")
def list(platform: Platform, project_id: str = None):
    response = get_predictions(
        product=platform.api_name if platform else None, project_id=project_id
    )
    if predictions := response.result:
        click.echo_via_pager(
            f"{p['created_at']} {p['prediction_id']}: {p['application_name']}\n"
            for p in predictions
        )
