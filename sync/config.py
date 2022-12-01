"""
Utilities providing configuration

Configuraiton is devided into 3 sections:
 - local configuration including the location of a potentially shared state file
 - credentials for the Sync API stored locally at ~/.sync/credentials
 - local or remote state configuration
"""

from pathlib import Path
from urllib.parse import urlparse, urlunparse

from .models import APIKey, Configuration


def configure(prediction_preference: str, state_url: str, api_key: APIKey):
    """
    Initializes configuration files. Currently only Linux-based systems are supported.
    """

    config_dir = get_config_dir()
    config_dir.mkdir(exist_ok=True)

    credentials_path = config_dir.joinpath("credentials")
    with open(credentials_path, "w") as credentials_out:
        credentials_out.write(api_key.json(indent=2, by_alias=True))

    parsed_url = urlparse(state_url or config_dir.joinpath("state").as_uri())
    config = Configuration(
        state_url=urlunparse(parsed_url), prediction_preference=prediction_preference
    )

    config_path = config_dir.joinpath("config")
    with open(config_path, "w") as config_out:
        config_out.write(config.json(indent=2))


def get_config_dir() -> Path:
    return Path("~/.sync").expanduser()


def get_api_key() -> APIKey:
    """Returns saved API key"""
    return APIKey.parse_file(get_config_dir().joinpath("credentials"))
