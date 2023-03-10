"""
Basic Sync CLI
"""

import logging

import click

from .cli import databricks, emr, predictions, projects
from .config import API_KEY, CONFIG, DB_CONFIG, APIKey, Configuration, DatabricksConf, init
from .models import Preference

OPTIONAL_DEFAULT = "none"


logging.basicConfig(level=logging.CRITICAL)


@click.group()
def main():
    pass


main.add_command(predictions.predictions)
main.add_command(projects.projects)
main.add_command(emr.emr)
main.add_command(databricks.databricks)


@main.command
def configure():
    api_key_id = click.prompt(f"API key ID", default=API_KEY.id if API_KEY else None)
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
        "Default predicion preference",
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


if __name__ == "__main__":
    main()
