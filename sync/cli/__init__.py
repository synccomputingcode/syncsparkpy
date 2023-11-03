"""
Basic Sync CLI
"""
import logging

import click

from sync.api.predictions import get_products
from sync.cli import awsdatabricks, awsemr, azuredatabricks, predictions, profiles, projects, workspaces
from sync.cli.util import OPTIONAL_DEFAULT, configure_profile
from sync.clients.sync import get_default_client

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
main.add_command(profiles.profiles)
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
@click.option("--profile", default="default")
def configure(
        api_key_id: str = None,
        api_key_secret: str = None,
        prediction_preference: str = None,
        databricks_host: str = None,
        databricks_token: str = None,
        databricks_region: str = None,
        profile: str = "default",
):
    configure_profile(api_key_id,
                      api_key_secret,
                      prediction_preference,
                      databricks_host,
                      databricks_token,
                      databricks_region,
                      profile)


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
