"""
Basic Sync CLI
"""

import logging

import click

from .cli import predictions
from .config import API_KEY, CONFIG, APIKey, Configuration, init
from .models import Preference

NOT_REQUIRED = "optional"


logging.basicConfig(level=logging.CRITICAL)


@click.group()
def main():
    pass


main.add_command(predictions.predictions)


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
        "Default project S3 URL", default=CONFIG.default_prediction_preference or NOT_REQUIRED
    )
    prediction_preference = click.prompt(
        "Default predicion preference",
        type=click.Choice([p.value for p in Preference]),
        default=(CONFIG.default_prediction_preference or Preference.BALANCED).value,
    )

    init(
        APIKey(api_key_id=api_key_id, api_key_secret=api_key_secret),
        Configuration(
            default_project_url=project_url if project_url != NOT_REQUIRED else None,
            default_prediction_preference=prediction_preference,
        ),
    )


if __name__ == "__main__":
    main()
