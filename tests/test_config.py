import unittest
import pytest
from unittest.mock import patch
from sync import config


class TestConfig(unittest.TestCase):
    @patch("sync.config._api_key")
    def test_set_api_key_when_api_key_exists_raises_an_error(
        self,
        mock_api_key
    ):
        mock_api_key.return_value = config.APIKey(
            api_key_id="mock_api_key_id",
            api_key_secret="mock_api_key_secret"
        )

        new_api_key = config.APIKey(
            api_key_id="mock_api_key_id_2",
            api_key_secret="mock_api_key_secret_2"
        )

        with pytest.raises(RuntimeError):
            config.set_api_key(new_api_key)

    @patch("sync.config._db_config")
    def test_set_databricks_config_when_db_config_exists_raises_an_error(
        self,
        mock_db_config
    ):
        mock_db_config.return_value = config.DatabricksConf(
            host="https://fakedbx-host.com",
            token="dsapifaketoken",
            aws_region_name="us-east-1"
        )

        new_db_config = config.DatabricksConf(
            host="https://fakedbx-host-2.com",
            token="dsapifaketoken2",
            aws_region_name="us-east-2"
        )

        with pytest.raises(RuntimeError):
            config.set_databricks_config(new_db_config)
