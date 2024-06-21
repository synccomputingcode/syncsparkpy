from unittest.mock import patch

import pytest
from pytest import raises

from sync.projects import NoSuccessfulSubmissionsFoundError, get_latest_submission_config

mock_base_configuration = {
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "custom_tags": {
        "Vendor": "Databricks",
        "Creator": "",
        "ClusterName": "job-951738965563208-run-13053153698407-Job_cluster",
        "ClusterId": "0315-161419-efa397m4",
        "JobId": "951738965563208",
    },
    "num_workers": 2,
    "spark_conf": {"spark.databricks.isv.product": "sync-gradient"},
    "spark_version": "13.3.x-scala2.12",
    "runtime_engine": "STANDARD",
    "azure_attributes": {
        "availability": "ON_DEMAND_AZURE",
        "first_on_demand": 1,
        "spot_bid_max_price": -1.0,
    },
}
mock_success_configuration = {"state": "SUCCESS", **mock_base_configuration}
mock_failed_configuration = {"state": "FAILED", **mock_base_configuration}

mock_base_submission = {
    "created_at": "2024-03-15T16:27:12Z",
    "updated_at": "2024-03-15T16:27:21Z",
    "id": "b7ee7431-3188-44ac-95fc-6f2068de39b6",
    "project_id": "ff10d866-ebf5-46ff-84dc-b5430a5eea45",
}
mock_success_submission = {
    **mock_base_submission,
    "state": "SUCCESS",
    "configuration": mock_success_configuration,
}
mock_failed_submission = {
    **mock_base_submission,
    "state": "FAILED",
    "configuration": mock_failed_configuration,
}


@pytest.fixture
def mock_mixed_submissions():
    yield {"items": [mock_failed_submission, mock_success_submission]}


@pytest.fixture
def mock_no_success_submissions():
    yield {"items": [mock_failed_submission]}


@pytest.fixture
def mock_get_submissions_empty():
    """Mocks get_submissions function to return an empty list."""
    yield {"items": []}


@patch("sync.clients.sync.SyncClient._send")
def test_get_latest_submission_config_success(mock_send, mock_mixed_submissions):
    """Test get_latest_submission_config returns the latest successful submission configuration."""
    mock_send.return_value = mock_mixed_submissions
    with patch("sync.api.projects.get_submissions", lambda x: mock_mixed_submissions):
        project_id = "ff10d866-ebf5-46ff-84dc-b5430a5eea45"
        config = get_latest_submission_config(project_id, success_only=True)
        assert "node_type_id" in config
        assert "Vendor" in config.get("custom_tags", "")
        assert config["state"] == "SUCCESS"


@patch("sync.clients.sync.SyncClient._send")
def test_get_latest_submission_config_failure_ok(mock_send, mock_mixed_submissions):
    mock_send.return_value = mock_mixed_submissions
    with patch("sync.api.projects.get_submissions", lambda x: mock_mixed_submissions):
        project_id = "ff10d866-ebf5-46ff-84dc-b5430a5eea45"
        config = get_latest_submission_config(project_id, success_only=False)
        assert "node_type_id" in config
        assert "Vendor" in config.get("custom_tags", "")
        assert config["state"] == "FAILED"


@patch("sync.clients.sync.SyncClient._send")
def test_get_latest_submission_raises_no_successful_submissions_found_error(
    mock_send, mock_no_success_submissions
):
    mock_send.return_value = mock_no_success_submissions
    with patch("sync.api.projects.get_submissions", lambda x: mock_no_success_submissions):
        project_id = "ff10d866-ebf5-46ff-84dc-b5430a5eea45"
        with raises(NoSuccessfulSubmissionsFoundError):
            get_latest_submission_config(project_id, success_only=True)
