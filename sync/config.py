"""
Utilities providing configuration to the SDK
"""

import json
from pathlib import Path
from typing import Any, Dict, Tuple, Type
from urllib.parse import urlparse

import boto3 as boto
from pydantic import AliasChoices, ConfigDict, Field, field_validator
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

CREDENTIALS_FILE = "credentials"
CONFIG_FILE = "config"
DATABRICKS_CONFIG_FILE = "databrickscfg"


class ConfigError(Exception):
    """Exception raised for errors in the config file."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class JSONConfigSettingsSource(PydanticBaseSettingsSource):
    def __init__(self, path: str, settings_cls: Type[BaseSettings]):
        super().__init__(settings_cls)
        self.path = path

    def get_field_value(self, field: FieldInfo, field_name: str) -> Tuple[Any, str, bool]:
        config_path = _get_config_dir().joinpath(self.path)
        if config_path.exists():
            with open(config_path) as fobj:
                content = fobj.read()
                if not content:
                    raise ConfigError(f"Config file '{self.path}' is empty.")
                data = json.loads(content)
                if field_name not in data:
                    raise ConfigError(f"Missing '{field_name}' in config file '{self.path}'.")
                field_value = data.get(field_name, None)
                return field_value, field_name, True
        return field.default, field_name, False

    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        return value

    def __call__(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}

        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(field, field_name)
            field_value = self.prepare_field_value(field_name, field, field_value, value_is_complex)
            if field_value is not None:
                d[field_key] = field_value
        return d


class APIKey(BaseSettings):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)
    api_key_id: str = Field(
        ...,
        alias="api_key_id",
        validation_alias=AliasChoices("SYNC_API_KEY_ID", "id"),  # , "api_key_id"),
        serialization_alias="api_key_id",
    )
    api_key_secret: str = Field(
        ...,
        alias="api_key_secret",
        validation_alias=AliasChoices("SYNC_API_KEY_SECRET", "secret"),  # , "api_key_secret"),
        serialization_alias="api_key_secret",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            JSONConfigSettingsSource(CREDENTIALS_FILE, settings_cls),
        )


class Configuration(BaseSettings):
    model_config = ConfigDict(
        frozen=True,
        populate_by_name=True,
        extra="ignore",
    )
    api_url: str = Field("https://api.synccomputing.com", validation_alias="SYNC_API_URL")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return init_settings, env_settings, JSONConfigSettingsSource(CONFIG_FILE, settings_cls)


class DatabricksConf(BaseSettings):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    host: str = Field(..., validation_alias="DATABRICKS_HOST")
    token: str = Field(..., validation_alias="DATABRICKS_TOKEN")
    aws_region_name: str = Field(
        boto.client("s3").meta.region_name, validation_alias="DATABRICKS_AWS_REGION"
    )

    @classmethod
    @field_validator("host")
    def validate_host(cls, host):

        if host:
            parsed_host = urlparse(host)
            return f"https://{parsed_host.netloc}"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            JSONConfigSettingsSource(DATABRICKS_CONFIG_FILE, settings_cls),
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
        credentials_out.write(api_key.model_dump_json(exclude_none=True, indent=2))
    global _api_key
    _api_key = api_key

    config_path = config_dir.joinpath(CONFIG_FILE)
    with open(config_path, "w") as config_out:
        config_out.write(config.model_dump_json(exclude_none=True, indent=2))
    global _config
    _config = config

    if db_config:
        db_config_path = config_dir.joinpath(DATABRICKS_CONFIG_FILE)
        with open(db_config_path, "w") as db_config_out:
            db_config_out.write(db_config.model_dump_json(exclude_none=True, indent=2))
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
