"""
Utilities providing configuration to the SDK
"""

import json
from pathlib import Path
from typing import Any, Callable, Dict, Union
from urllib.parse import urlparse

import boto3 as boto
from pydantic import BaseSettings, Field, validator, Extra

from .models import Preference

CREDENTIALS_FILE = "credentials"
CONFIG_FILE = "config"
DATABRICKS_CONFIG_FILE = "databrickscfg"


def json_config_settings_source(path: str, profile: str) -> Callable[[BaseSettings], Dict[str, Any]]:
    def source(settings: BaseSettings) -> Dict[str, Any]:
        profile_dir = _get_profile_dir(profile)
        config_path = profile_dir.joinpath(path)
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
            return init_settings, env_settings, json_config_settings_source(CREDENTIALS_FILE, get_profile_name())


class Configuration(BaseSettings):
    default_prediction_preference: Union[Preference, None] = Preference.ECONOMY
    api_url: str = Field("https://api.synccomputing.com", env="SYNC_API_URL")

    class Config:
        extra = Extra.ignore

        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return init_settings, env_settings, json_config_settings_source(CONFIG_FILE, get_profile_name())


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
                json_config_settings_source(DATABRICKS_CONFIG_FILE, get_profile_name()),
            )


def init(api_key: APIKey, config: Configuration, db_config: DatabricksConf = None, profile: str = "default"):
    """Initializes configuration files. Currently only Linux-based systems are supported.

    :param api_key: API key
    :type api_key: APIKey
    :param config: configuration
    :type config: Configuration
    :param db_config: Databricks configuration, defaults to None
    :type db_config: DatabricksConf, optional
    :param profile: profile name, defaults to "default"
    :type profile: str, optional
    """
    global _active_profile
    _active_profile = profile

    config_dir = _get_profile_dir(profile)
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
    set_profile(_active_profile)


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
    """Gets Databricks configuration

    :return: Databricks configuration
    :rtype: DatabricksConf
    """
    global _db_config
    if _db_config is None:
        try:
            _db_config = DatabricksConf()
        except ValueError:
            pass
    return _db_config


def get_profile_name() -> str:
    """Gets the active profile

    :return: active profile
    :rtype: str
    """
    global _active_profile
    _active_profile = _current_profile_dir().resolve().name
    if not _active_profile:
        raise ValueError("No active profile found")
    return _active_profile


CONFIG: Configuration
_config = None
API_KEY: APIKey
_api_key = None
DB_CONFIG: DatabricksConf
_db_config = None
# PROFILE: str
_active_profile: str = "default"


def __getattr__(name):
    if name == "CONFIG":
        return get_config()
    elif name == "API_KEY":
        return get_api_key()
    elif name == "DB_CONFIG":
        return get_databricks_config()
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def clear_configurations():
    global _config, _api_key, _db_config
    _config = None
    _api_key = None
    _db_config = None


def refresh_configurations(profile_name: str):
    """Refreshes the global configurations to reflect the new active profile."""
    global _active_profile, _api_key, _config, _db_config

    _active_profile = profile_name
    _api_key = None
    _config = None
    _db_config = None

    _api_key = get_api_key()
    _config = get_config()
    _db_config = get_databricks_config()


def create_symlink(profile_name: str = "default"):
    """Creates a symlink to the current profile."""
    current_symlink = Path("~/.sync/profiles/current").expanduser()
    target_dir = _get_profile_dir(profile_name).resolve()
    if current_symlink.is_symlink():
        current_symlink.unlink()
    current_symlink.symlink_to(target_dir, target_is_directory=True)
    

def set_profile(profile_name: str):
    """Sets the active profile."""
    global _active_profile
    _active_profile = profile_name
    create_symlink(_active_profile)
    refresh_configurations(_active_profile)


def _get_profile_dir(profile: str) -> Path:
    return Path(f"~/.sync/profiles/{profile}").expanduser()


def _current_profile_dir() -> Path:
    return Path(f"~/.sync/profiles/current").expanduser().resolve()
