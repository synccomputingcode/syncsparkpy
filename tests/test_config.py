import unittest
import pytest

from sync import config


class TestConfig(unittest.TestCase):
    def test_set_api_key_twice_raises_an_error(
        self
    ):
        mock_api_key = config.APIKey(
            api_key_id="mock_api_key_id",
            api_key_secret="mock_api_key_secret"
        )
        config.set_api_key(mock_api_key)

        mock_api_key_2 = config.APIKey(
            api_key_id="mock_api_key_id_2",
            api_key_secret="mock_api_key_secret_2"
        )

        with pytest.raises(RuntimeError):
            config.set_api_key(mock_api_key_2)

    def test_set_databricks_config_twice_raises_an_error(
        self
    ):
        mock_dbx_config = config.DatabricksConf(
            host="https://fakedbx-host.com",
            token="dsapifaketoken",
            aws_region_name="us-east-1"
        )
        config.set_databricks_config(mock_dbx_config)

        mock_dbx_config_2 = config.DatabricksConf(
            host="https://fakedbx-host-2.com",
            token="dsapifaketoken2",
            aws_region_name="us-east-2"
        )

        with pytest.raises(RuntimeError):
            config.set_databricks_config(mock_dbx_config_2)
