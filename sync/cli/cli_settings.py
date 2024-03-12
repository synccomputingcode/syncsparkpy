"""
Basic Sync CLI
"""

import click

from sync.config import get_api_key, get_config, get_databricks_config


@click.group()
def settings():
    """Sync CLI"""
    pass


def api_settings():
    api_key = get_api_key()
    if not api_key:
        click.echo("No API key set")
        return
    click.echo(f"API Key ID: {api_key.api_key_id}")
    secret = obfuscate_secret(api_key.api_key_secret)
    click.echo(f"API Key Secret: {secret}")


@settings.command
def show_api_key():
    """Show API key"""
    api_settings()


def databricks_settings():
    db_config = get_databricks_config()
    if not db_config:
        click.echo("No Databricks config set")
        return
    click.echo(f"Databricks Host: {db_config.host}")
    token = obfuscate_secret(db_config.token)
    click.echo(f"Databricks Token: {token}")
    click.echo(f"AWS Region: {db_config.aws_region_name}")


@settings.command
def show_databricks_config():
    """Show Databricks config"""
    databricks_settings()


def config_settings():
    conf = get_config()
    if not conf:
        click.echo("No configuration set")
        return
    click.echo(f"api_url: {conf.api_url}")


@settings.command
def show_config():
    """Show credentials"""
    config_settings()


@settings.command()
def show_all():
    """Show all configurations"""
    api_settings()
    databricks_settings()
    config_settings()


def obfuscate_secret(secret: str) -> str:
    if secret:
        if len(secret) < 20:
            return f"{'*' * len(secret)}"
        else:
            return f"{secret[:3]}{'*' * (len(secret) - 7)}{secret[-4:]}"
    return ""
