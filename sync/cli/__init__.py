"""
Basic Sync CLI
"""
import logging

import click

from sync.api.predictions import get_products
from sync.cli import awsdatabricks, awsemr, azuredatabricks, predictions, projects, workspaces
from sync.cli.util import OPTIONAL_DEFAULT
from sync.clients.sync import get_default_client
from sync.config import API_KEY, CONFIG, DB_CONFIG, APIKey, Configuration, DatabricksConf, init
from sync.models import Preference

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


@click.group()
@click.option("--debug", is_flag=True, default=False, help="Enable logging.")
@click.version_option(package_name="syncsparkpy")
def main(debug: bool):
    if debug:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    else:
        logging.disable()


main.add_command(predictions.predictions)
main.add_command(projects.projects)
main.add_command(awsemr.aws_emr)
main.add_command(awsdatabricks.aws_databricks)
main.add_command(azuredatabricks.azure_databricks)
main.add_command(workspaces.workspaces)


@main.command
@click.option("--api-key-id")
@click.option("--api-key-secret")
@click.option("--prediction-preference")
@click.option("--databricks-host")
@click.option("--databricks-token")
@click.option("--databricks-region")
def configure(
    api_key_id: str = None,
    api_key_secret: str = None,
    prediction_preference: str = None,
    databricks_host: str = None,
    databricks_token: str = None,
    databricks_region: str = None,
):
    """Configure Sync Library"""
    api_key_id = api_key_id or click.prompt(
        "Sync API key ID", default=API_KEY.id if API_KEY else None
    )
    api_key_secret = api_key_secret or click.prompt(
        "Sync API key secret",
        default=API_KEY.secret if API_KEY else None,
        hide_input=True,
        show_default=False,
    )

    prediction_preference = prediction_preference or click.prompt(
        "Default prediction preference",
        type=click.Choice([p.value for p in Preference]),
        default=(CONFIG.default_prediction_preference or Preference.ECONOMY).value,
    )

    dbx_host = databricks_host or OPTIONAL_DEFAULT
    dbx_token = databricks_token or OPTIONAL_DEFAULT
    dbx_region = databricks_region or OPTIONAL_DEFAULT
    # Skip only if all are provided since all are required to initialize the configuration below
    if any(param == OPTIONAL_DEFAULT for param in (dbx_host, dbx_token, dbx_region)):
        if click.confirm("Would you like to configure a Databricks workspace?"):
            dbx_host = click.prompt(
                "Databricks host (prefix with https://)",
                default=DB_CONFIG.host if DB_CONFIG else OPTIONAL_DEFAULT,
            )
            dbx_token = click.prompt(
                "Databricks token",
                default=DB_CONFIG.token if DB_CONFIG else OPTIONAL_DEFAULT,
                hide_input=True,
                show_default=False,
            )
            dbx_region = click.prompt(
                "Databricks AWS region name",
                default=DB_CONFIG.aws_region_name if DB_CONFIG else OPTIONAL_DEFAULT,
            )

    init(
        APIKey(api_key_id=api_key_id, api_key_secret=api_key_secret),
        Configuration(
            default_prediction_preference=prediction_preference,
        ),
        DatabricksConf(host=dbx_host, token=dbx_token, aws_region_name=dbx_region)
        if dbx_host != OPTIONAL_DEFAULT
        and dbx_token != OPTIONAL_DEFAULT
        and dbx_region != OPTIONAL_DEFAULT
        else None,
    )


@main.command
def products():
    """List supported products"""
    products_response = get_products()
    products = products_response.result
    if products:
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
