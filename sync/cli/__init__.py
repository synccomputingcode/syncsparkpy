"""
Basic Sync CLI
"""

import logging
from typing import Optional

import click

from sync.api.projects import get_products
from sync.cli import awsdatabricks, azuredatabricks, projects, workspaces
from sync.cli.util import OPTIONAL_DEFAULT
from sync.clients.sync import get_default_client
from sync.config import API_KEY, CONFIG, DB_CONFIG, APIKey, Configuration, DatabricksConf, init

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


@click.group()
@click.option("--debug", is_flag=True, default=False, help="Enable logging.")
@click.version_option(package_name="syncsparkpy")
def main(debug: bool):
    if debug:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    else:
        logging.disable()


main.add_command(projects.projects)
main.add_command(awsdatabricks.aws_databricks)
main.add_command(azuredatabricks.azure_databricks)
main.add_command(workspaces.workspaces)


@main.command
@click.option("--api-key-id")
@click.option("--api-key-secret")
@click.option("--databricks-host")
@click.option("--databricks-token")
@click.option("--databricks-region")
def configure(
    api_key_id: Optional[str] = None,
    api_key_secret: Optional[str] = None,
    databricks_host: Optional[str] = None,
    databricks_token: Optional[str] = None,
    databricks_region: Optional[str] = None,
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
        Configuration(api_url=CONFIG.api_url if CONFIG else "https://api.synccomputing.com"),
        (
            DatabricksConf(host=dbx_host, token=dbx_token, aws_region_name=dbx_region)
            if dbx_host != OPTIONAL_DEFAULT
            and dbx_token != OPTIONAL_DEFAULT
            and dbx_region != OPTIONAL_DEFAULT
            else None
        ),
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
