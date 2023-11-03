from typing import Union
from uuid import UUID

import click

from sync.api.projects import get_project_by_app_id
from sync.config import API_KEY, CONFIG, DB_CONFIG, APIKey, Configuration, DatabricksConf, init
from sync.models import Preference

OPTIONAL_DEFAULT = "none"


def validate_project(ctx, param, value: Union[str, None]) -> dict:
    if not value:
        return {"id": None}

    try:
        UUID(value)
        return {"id": value}
    except ValueError:
        project_response = get_project_by_app_id(value)
        error = project_response.error
        if error:
            ctx.fail(str(error))

        return project_response.result


def configure_profile(
        api_key_id: str = None,
        api_key_secret: str = None,
        prediction_preference: str = None,
        databricks_host: str = None,
        databricks_token: str = None,
        databricks_region: str = None,
        profile: str = "default",
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
        profile=profile,
    )
