"""
Utilities providing configuration to the SDK
"""

import json
from pathlib import Path
from typing import Any, Callable, Dict
from urllib.parse import urlparse

import boto3 as boto
from pydantic import field_validator, Field, ConfigDict
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

CREDENTIALS_FILE = "credentials"
CONFIG_FILE = "config"
DATABRICKS_CONFIG_FILE = "databrickscfg"


def json_config_settings_source(path: str) -> Callable[[BaseSettings], Dict[str, Any]]:
    def source(settings: BaseSettings) -> Dict[str, Any]:
        config_path = _get_config_dir().joinpath(path)
        if config_path.exists():
            with open(config_path) as fobj:
                return json.load(fobj)
        return {}

    return source


class APIKey(BaseSettings):
    id: str = Field(..., alias="api_key_id", validation_alias="SYNC_API_KEY_ID")
    secret: str = Field(..., alias="api_key_secret", validation_alias="SYNC_API_KEY_SECRET")

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[
        PydanticBaseSettingsSource,
        PydanticBaseSettingsSource,
        Callable[[BaseSettings], dict[str, Any]],
    ]:
        return init_settings, env_settings, json_config_settings_source(CREDENTIALS_FILE)


class Configuration(BaseSettings):
    api_url: str = Field("https://api.synccomputing.com", validation_alias="SYNC_API_URL")

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[
        PydanticBaseSettingsSource,
        PydanticBaseSettingsSource,
        Callable[[BaseSettings], dict[str, Any]],
    ]:
        return init_settings, env_settings, json_config_settings_source(CONFIG_FILE)


class DatabricksConf(BaseSettings):
    host: str = Field(..., validation_alias="DATABRICKS_HOST")
    token: str = Field(..., validation_alias="DATABRICKS_TOKEN")
    aws_region_name: str = Field(
        boto.client("s3").meta.region_name, validation_alias="DATABRICKS_AWS_REGION"
    )

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    @classmethod
    @field_validator("host")
    def validate_host(cls, host):

        if host:
            parsed_host = urlparse(host)
            return f"https://{parsed_host.netloc}"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[
        PydanticBaseSettingsSource,
        PydanticBaseSettingsSource,
        Callable[[BaseSettings], dict[str, Any]],
    ]:
        return init_settings, env_settings, json_config_settings_source(DATABRICKS_CONFIG_FILE)


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
    if _api_key is None:
        try:
            _api_key = APIKey()
        except ValueError:
            pass
    return _api_key


def set_api_key(api_key: APIKey):
    global _api_key
    if _api_key is not None:
        raise RuntimeError(
            "Sync API key/secret has already been set and the library does not support resetting "
            "credentials"
        )
    _api_key = api_key


def get_config() -> Configuration:
    """Gets configuration

    :return: configuration
    :rtype: Configuration
    """
    global _config
    if _config is None:
        _config = Configuration()
    return _config


def get_databricks_config() -> DatabricksConf:
    global _db_config
    if _db_config is None:
        try:
            _db_config = DatabricksConf()
        except ValueError:
            pass
    return _db_config


def set_databricks_config(db_config: DatabricksConf):
    global _db_config
    if _db_config is not None:
        raise RuntimeError(
            "Databricks config has already been set and the library does not support resetting "
            "credentials"
        )
    _db_config = db_config


CONFIG: Configuration
_config = None
API_KEY: APIKey
_api_key = None
DB_CONFIG: DatabricksConf
_db_config = None


def __getattr__(name):
    if name == "CONFIG":
        return get_config()
    elif name == "API_KEY":
        return get_api_key()
    elif name == "DB_CONFIG":
        return get_databricks_config()
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _get_config_dir() -> Path:
    return Path("~/.sync").expanduser()
