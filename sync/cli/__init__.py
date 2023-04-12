"""
Basic Sync CLI
"""
import logging

import click

from sync.api.predictions import get_products
from sync.cli import awsdatabricks, awsemr, predictions, projects
from sync.clients.sync import get_default_client
from sync.config import API_KEY, CONFIG, DB_CONFIG, APIKey, Configuration, DatabricksConf, init
from sync.models import Preference

OPTIONAL_DEFAULT = "none"

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


@click.group()
@click.option("--debug", is_flag=True, default=False, help="Enable logging.")
@click.version_option(package_name="syncsparkpy")
def main(debug: bool):
    if debug:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    else:
        logging.basicConfig(level=logging.CRITICAL, format=LOG_FORMAT)


main.add_command(predictions.predictions)
main.add_command(projects.projects)
main.add_command(awsemr.aws_emr)
main.add_command(awsdatabricks.aws_databricks)


@main.command
def configure():
    """Configure Sync Library"""
    api_key_id = click.prompt("API key ID", default=API_KEY.id if API_KEY else None)
    api_key_secret = click.prompt(
        "API key secret",
        default=API_KEY.secret if API_KEY else None,
        hide_input=True,
        show_default=False,
    )

    project_url = click.prompt(
        "Default project S3 URL", default=CONFIG.default_project_url or OPTIONAL_DEFAULT
    )
    prediction_preference = click.prompt(
        "Default prediction preference",
        type=click.Choice([p.value for p in Preference]),
        default=(CONFIG.default_prediction_preference or Preference.BALANCED).value,
    )

    db_host = click.prompt(
        "Databricks host (prefix with https://)",
        default=DB_CONFIG.host if DB_CONFIG else OPTIONAL_DEFAULT,
    )
    db_token = click.prompt(
        "Databricks token",
        default=DB_CONFIG.token if DB_CONFIG else OPTIONAL_DEFAULT,
        hide_input=True,
        show_default=False,
    )

    init(
        APIKey(api_key_id=api_key_id, api_key_secret=api_key_secret),
        Configuration(
            default_project_url=project_url if project_url != OPTIONAL_DEFAULT else None,
            default_prediction_preference=prediction_preference,
        ),
        DatabricksConf(host=db_host, token=db_token)
        if db_host != OPTIONAL_DEFAULT and db_token != OPTIONAL_DEFAULT
        else None,
    )


@main.command
def products():
    """List supported products"""
    products_response = get_products()
    if products := products_response.result:
        click.echo(", ".join(products))
    else:
        click.echo(str(products_response.error), err=True)


@main.command
def token():
    """Get an API access token"""
    sync_client = get_default_client()
    response = sync_client.get_products()
    if "result" in response:
        click.echo(sync_client._client.auth._access_token)
    else:
        click.echo(f"{response['error']['code']}: {response['error']['message']}", err=True)
