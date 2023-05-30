"""
Utilities providing configuration to the SDK
"""

import json
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import boto3 as boto
from pydantic import BaseSettings, Field, validator

from .models import Preference

CREDENTIALS_FILE = "credentials"
CONFIG_FILE = "config"
DATABRICKS_CONFIG_FILE = "databrickscfg"


def json_config_settings_source(path: str) -> Callable[[BaseSettings], dict[str, Any]]:
    def source(settings: BaseSettings) -> dict[str, Any]:
        config_path = _get_config_dir().joinpath(path)
        if config_path.exists():
            with open(config_path) as fobj:
                return json.load(fobj)
        return {}

    return source


class APIKey(BaseSettings):
    id: str = Field(..., alias="api_key_id", env="SYNC_API_KEY_ID")
    secret: str = Field(..., alias="api_key_secret", env="SYNC_API_KEY_SECRET")

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (init_settings, env_settings, json_config_settings_source(CREDENTIALS_FILE))


class Configuration(BaseSettings):
    default_project_url: str | None = Field(description="default location for Sync project data")
    default_prediction_preference: Preference | None = Preference.BALANCED
    api_url: str = Field("https://api.synccomputing.com", env="SYNC_API_URL")

    @validator("default_project_url")
    def validate_url(cls, url):
        if url:
            # There are valid S3 URLs (e.g. with spaces) not supported by Pydantic URL types: https://docs.pydantic.dev/usage/types/#urls
            # Hence the manual validation here
            parsed_url = urlparse(url.rstrip("/"))

            if parsed_url.scheme != "s3":
                raise ValueError("Only S3 URLs please!")

            return parsed_url.geturl()

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (init_settings, env_settings, json_config_settings_source(CONFIG_FILE))


class DatabricksConf(BaseSettings):
    host: str = Field(..., env="DATABRICKS_HOST")
    token: str = Field(..., env="DATABRICKS_TOKEN")
    aws_region_name: str = Field(boto.client("s3").meta.region_name, env="DATABRICKS_AWS_REGION")

    @validator("host")
    def validate_host(cls, host):
        if host:
            parsed_host = urlparse(host)
            return f"https://{parsed_host.netloc}"

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (
                init_settings,
                env_settings,
                json_config_settings_source(DATABRICKS_CONFIG_FILE),
            )


def init(api_key: APIKey, config: Configuration, db_config: DatabricksConf = None):
    """Initializes configuration files. Currently only Linux-based systems are supported.

    :param api_key: API key
    :type api_key: APIKey
    :param config: configuration
    :type config: Configuration
    :param db_config: Databricks configuration, defaults to None
    :type db_config: DatabricksConf, optional
    """
    config_dir = _get_config_dir()
    config_dir.mkdir(exist_ok=True)

    credentials_path = config_dir.joinpath(CREDENTIALS_FILE)
    with open(credentials_path, "w") as credentials_out:
        credentials_out.write(api_key.json(by_alias=True, indent=2))
    global _api_key
    _api_key = api_key

    config_path = config_dir.joinpath(CONFIG_FILE)
    with open(config_path, "w") as config_out:
        config_out.write(config.json(exclude_none=True, indent=2))
    global _config
    _config = config

    if db_config:
        db_config_path = config_dir.joinpath(DATABRICKS_CONFIG_FILE)
        with open(db_config_path, "w") as db_config_out:
            db_config_out.write(db_config.json(exclude_none=True, indent=2))
        global _db_config
        _db_config = db_config


def get_api_key() -> APIKey:
    """Returns API key from configuration

    :return: API key
    :rtype: APIKey
    """
    global _api_key
    if not _api_key:
        _api_key = APIKey()
    return _api_key


def get_config() -> Configuration:
    """Gets configuration

    :return: configuration
    :rtype: Configuration
    """
    global _config
    if not _config:
        _config = Configuration()
    return _config


CONFIG: Configuration
_config = None
API_KEY: APIKey
_api_key = None
DB_CONFIG: DatabricksConf
_db_config = None


def __getattr__(name):
    if name == "CONFIG":
        global _config
        if _config is None:
            _config = Configuration()
        return _config
    elif name == "API_KEY":
        global _api_key
        if _api_key is None:
            try:
                _api_key = APIKey()
            except ValueError:
                pass
        return _api_key
    elif name == "DB_CONFIG":
        global _db_config
        if _db_config is None:
            try:
                _db_config = DatabricksConf()
            except ValueError:
                pass
        return _db_config
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _get_config_dir() -> Path:
    return Path("~/.sync").expanduser()
