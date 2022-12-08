"""
Utilities providing configuration to the SDK
"""

from pathlib import Path

from .models import APIKey, Configuration


def configure(api_key: APIKey, config: Configuration):
    """Initializes configuration files. Currently only Linux-based systems are supported.

    :param api_key: API key
    :type api_key: APIKey
    :param config: configuration
    :type config: Configuration
    """
    config_dir = _get_config_dir()
    config_dir.mkdir(exist_ok=True)

    credentials_path = config_dir.joinpath("credentials")
    with open(credentials_path, "w") as credentials_out:
        credentials_out.write(api_key.json(indent=2, by_alias=True))

    config_path = config_dir.joinpath("config")
    with open(config_path, "w") as config_out:
        config_out.write(config.json(indent=2))


def get_api_key() -> APIKey:
    """Returns API key from configuration

    :return: API key
    :rtype: APIKey
    """
    return APIKey.parse_file(_get_config_dir().joinpath("credentials"))


def _get_config_dir() -> Path:
    return Path("~/.sync").expanduser()
