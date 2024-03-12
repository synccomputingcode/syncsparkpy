import json

import pytest

from sync import config
from sync.config import (
    CONFIG_FILE,
    CREDENTIALS_FILE,
    DATABRICKS_CONFIG_FILE,
    get_api_key,
    get_config,
    get_databricks_config,
    set_api_key,
)


@pytest.fixture(scope="function", autouse=True)
def reset_configs():
    config._api_key = None
    config._config = None
    config._db_config = None
    yield
    config._api_key = None
    config._config = None
    config._db_config = None


@pytest.fixture(scope="function", autouse=True)
def patch_config_dir(tmp_path, mocker):
    config_dir = "sync.config._get_config_dir"
    with mocker.patch(config_dir, return_value=tmp_path):
        yield


@pytest.fixture(scope="function")
def unset_env_vars(monkeypatch):
    vars_to_unset = [
        "SYNC_API_KEY_ID",
        "SYNC_API_KEY_SECRET",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_AWS_REGION",
        "SYNC_API_URL",
    ]
    for var in vars_to_unset:
        monkeypatch.delenv(var, raising=False)
    yield


@pytest.fixture(scope="function")
def set_env_vars(monkeypatch):
    env_vars = {
        "SYNC_API_KEY_ID": "env_test_id",
        "SYNC_API_KEY_SECRET": "env_test_secret",
        "DATABRICKS_HOST": "https://envtestdatabricks.com",
        "DATABRICKS_TOKEN": "env_test_token",
        "DATABRICKS_AWS_REGION": "us-east-1",
        "SYNC_API_URL": "https://envtest.synccomputing.com",
    }
    for var, value in env_vars.items():
        monkeypatch.setenv(var, value)
    yield


class TestSetConfig:
    @pytest.fixture(scope="function")
    def new_api_key(self):
        yield config.APIKey(api_key_id="mock_api_key_id_2", api_key_secret="mock_api_key_secret_2")

    @pytest.fixture(scope="class")
    def new_db_config(self):
        yield config.DatabricksConf(
            host="https://fakedbx-host-2.com", token="dapifaketoken2", aws_region_name="us-east-2"
        )

    def test_set_api_key_when_api_key_exists_raises_an_error(
        self, mocker, patch_config_dir, new_api_key
    ):

        mocker.patch(
            "sync.config._api_key",
            return_value=config.APIKey(
                api_key_id="mock_api_key_id", api_key_secret="mock_api_key_secret"
            ),
        )

        with pytest.raises(RuntimeError):
            set_api_key(new_api_key)

    def test_set_api_key(self, new_api_key, monkeypatch, patch_config_dir, unset_env_vars):
        set_api_key(new_api_key)
        assert get_api_key() == new_api_key

    def test_set_databricks_config_when_db_config_exists_raises_an_error(
        self, mocker, new_db_config, monkeypatch, patch_config_dir, unset_env_vars
    ):
        mocker.patch(
            "sync.config._db_config",
            return_value=config.DatabricksConf(
                host="https://fakedbx-host.com", token="dsapifaketoken", aws_region_name="us-east-1"
            ),
        )

        with pytest.raises(RuntimeError):
            config.set_databricks_config(new_db_config)

    def test_set_databricks_config(
        self, new_db_config, monkeypatch, patch_config_dir, unset_env_vars
    ):
        config.set_databricks_config(new_db_config)
        assert config.get_databricks_config() == new_db_config


class TestGetWithoutEnvVars:
    @pytest.mark.parametrize(
        "env_setup, expected_id, expected_secret",
        [
            ("unset_env_vars", "file_test_id", "file_test_secret"),
            ("set_env_vars", "env_test_id", "env_test_secret"),
        ],
    )
    def test_get_api_key(self, request, tmp_path, env_setup, expected_id, expected_secret):
        request.getfixturevalue(env_setup)
        api_key_data = {"api_key_id": expected_id, "api_key_secret": expected_secret}
        credentials_file = tmp_path / CREDENTIALS_FILE
        credentials_file.write_text(json.dumps(api_key_data))
        api_key = get_api_key()

        assert api_key.api_key_id == expected_id
        assert api_key.api_key_secret == expected_secret

    @pytest.mark.parametrize(
        "env_setup, expected_url",
        [
            ("unset_env_vars", "https://filetest.synccomputing.com"),
            ("set_env_vars", "https://envtest.synccomputing.com"),
        ],
    )
    def test_get_config(self, request, tmp_path, env_setup, expected_url):
        request.getfixturevalue(env_setup)
        config_data = {"api_url": expected_url}
        config_file = tmp_path / CONFIG_FILE
        config_file.write_text(json.dumps(config_data))
        conf = get_config()
        assert conf.api_url == expected_url

    @pytest.mark.parametrize(
        "env_setup, expected_host, expected_token, expected_region",
        [
            ("unset_env_vars", "https://filetestdatabricks.com", "file_test_token", "us-west-1"),
            ("set_env_vars", "https://envtestdatabricks.com", "env_test_token", "us-east-1"),
        ],
    )
    def test_get_databricks_config(
        self, request, tmp_path, env_setup, expected_host, expected_token, expected_region
    ):
        request.getfixturevalue(env_setup)
        db_config_data = {
            "host": expected_host,
            "token": expected_token,
            "aws_region_name": expected_region,
        }
        db_config_file = tmp_path / DATABRICKS_CONFIG_FILE
        db_config_file.write_text(json.dumps(db_config_data))
        db_config = get_databricks_config()
        assert db_config.host == expected_host
        assert db_config.token == expected_token
        assert db_config.aws_region_name == expected_region
