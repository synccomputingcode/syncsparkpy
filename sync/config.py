"""
Utilities providing configuration to the SDK
"""

import json
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from pydantic import BaseSettings, Field, validator

from .models import Preference

CREDENTIALS_FILE = "credentials"
CONFIG_FILE = "config"


def json_config_settings_source(path: str) -> Callable[[BaseSettings], dict[str, Any]]:
    def source(settings: BaseSettings) -> dict[str, Any]:
        with open(_get_config_dir().joinpath(path)) as fobj:
            return json.load(fobj)

    return source


class APIKey(BaseSettings):
    id: str = Field(..., alias="api_key_id", env="SYNC_API_KEY_ID")
    secret: str = Field(..., alias="api_key_secret", env="SYNC_API_KEY_SECRET")

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (init_settings, json_config_settings_source(CREDENTIALS_FILE))


class Configuration(BaseSettings):
    default_project_url: str | None = Field(description="default location for Sync project data")
    default_prediction_preference: Preference | None
    api_url: str = Field("https://api.synccomputing.com", env="SYNC_API_URL")

    @validator("default_project_url")
    def validate_url(cls, url):
        # There are valid S3 URLs (e.g. with spaces) not supported by Pydantic URL types: https://docs.pydantic.dev/usage/types/#urls
        # Hence the manual validation here
        parsed_url = urlparse(url)

        if parsed_url.scheme != "s3":
            raise ValueError("Only S3 URLs please!")

        return url

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (init_settings, json_config_settings_source(CONFIG_FILE))


def configure(api_key: APIKey, config: Configuration):
    """Initializes configuration files. Currently only Linux-based systems are supported.

    :param api_key: API key
    :type api_key: APIKey
    :param config: configuration
    :type config: Configuration
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
    global _configuration
    _configuration = config


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
    global _configuration
    if not _configuration:
        _configuration = Configuration()
    return _configuration


CONFIG: Configuration
_config = None
API_KEY: APIKey
_api_key = None


def __getattr__(name):
    global _config
    if name == "CONFIG":
        if _config is None:
            _config = Configuration()
        return _config
    global _api_key
    if name == "API_KEY":
        if _api_key is None:
            try:
                _api_key = APIKey()
            except ValueError:
                pass
        return _api_key

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _get_config_dir() -> Path:
    return Path("~/.sync").expanduser()
