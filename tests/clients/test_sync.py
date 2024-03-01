from unittest import TestCase, mock
from unittest.mock import patch
import json

import pytest

from sync.clients.sync import SyncClient
from sync.config import APIKey


@pytest.fixture()
def mock_get_recommendation(request):
    with patch(
        "sync.clients.sync.SyncClient.get_latest_project_recommendation",
        side_effect=get_rec_from_file,
    ):
        yield


def get_rec_from_file():
    with open("tests/test_files/recommendation.json") as rec_in:
        return json.loads(rec_in.read())


class TestSync(TestCase):
    @mock.patch("sync.clients.sync.SyncClient._send")
    def test_get_latest_project_config_recommendation(self, mock_send):
        mock_send.return_value = get_rec_from_file()
        test_client = SyncClient("url", APIKey())
        expected_result = {
            "node_type_id": "i6.xlarge",
            "driver_node_type_id": "i6.xlarge",
            "custom_tags": {
                "sync:project-id": "b9bd7136-7699-4603-9040-c6dc4c914e43",
                "sync:run-id": "e96401da-f64d-4ed0-8ded-db1317f40248",
                "sync:recommendation-id": "e029a220-c6a5-49fd-b7ed-7ea046366741",
                "sync:tenant-id": "352176a7-b605-4cc2-b3b2-ee591715b6b4",
            },
            "num_workers": 20,
            "spark_conf": {"spark.databricks.isv.product": "sync-gradient"},
            "spark_version": "13.3.x-scala2.12",
            "runtime_engine": "PHOTON",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 100,
            },
        }

        result = test_client.get_latest_project_config_recommendation("project_id")
        assert result == expected_result

    @mock.patch("sync.clients.sync.SyncClient._send")
    def test_get_cluster_definition_and_recommendation(self, mock_send):
        mock_send.return_value = get_rec_from_file()
        test_client = SyncClient("url", APIKey())

        expected_result = {
            "cluster_recommendations": {
                "node_type_id": "i6.xlarge",
                "driver_node_type_id": "i6.xlarge",
                "custom_tags": {
                    "sync:project-id": "b9bd7136-7699-4603-9040-c6dc4c914e43",
                    "sync:run-id": "e96401da-f64d-4ed0-8ded-db1317f40248",
                    "sync:recommendation-id": "e029a220-c6a5-49fd-b7ed-7ea046366741",
                    "sync:tenant-id": "352176a7-b605-4cc2-b3b2-ee591715b6b4",
                },
                "num_workers": 20,
                "spark_conf": {"spark.databricks.isv.product": "sync-gradient"},
                "spark_version": "13.3.x-scala2.12",
                "runtime_engine": "PHOTON",
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "spot_bid_price_percent": 100,
                },
            },
            "cluster_definition": {
                "cluster_id": "1234-567890-reef123",
                "spark_context_id": 4020997813441462000,
                "cluster_name": "my-cluster",
                "spark_version": "13.3.x-scala2.12",
                "aws_attributes": {
                    "zone_id": "us-west-2c",
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0,
                },
                "node_type_id": "i3.xlarge",
                "driver_node_type_id": "i3.xlarge",
                "autotermination_minutes": 120,
                "enable_elastic_disk": False,
                "disk_spec": {"disk_count": 0},
                "cluster_source": "UI",
                "enable_local_disk_encryption": False,
                "instance_source": {"node_type_id": "i3.xlarge"},
                "driver_instance_source": {"node_type_id": "i3.xlarge"},
                "state": "TERMINATED",
                "state_message": "Inactive cluster terminated (inactive for 120 minutes).",
                "start_time": 1618263108824,
                "terminated_time": 1619746525713,
                "last_state_loss_time": 1619739324740,
                "num_workers": 30,
                "default_tags": {
                    "Vendor": "Databricks",
                    "Creator": "someone@example.com",
                    "ClusterName": "my-cluster",
                    "ClusterId": "1234-567890-reef123",
                },
                "creator_user_name": "someone@example.com",
                "termination_reason": {
                    "code": "INACTIVITY",
                    "parameters": {"inactivity_duration_min": "120"},
                    "type": "SUCCESS",
                },
                "init_scripts_safe_mode": False,
                "spec": {"spark_version": "13.3.x-scala2.12"},
            },
        }

        with open("tests/test_files/cluster.json") as cluster_in:
            result = test_client.get_cluster_definition_and_recommendation(
                "project_id", cluster_in.read()
            )
            assert result == expected_result

    @mock.patch("sync.clients.sync.SyncClient.get_latest_project_recommendation")
    def test_get_updated_cluster_defintion(self, mock_send):
        mock_send.return_value = get_rec_from_file()
        test_client = SyncClient("url", APIKey())

        expected_result = {
            "cluster_id": "1234-567890-reef123",
            "spark_context_id": 4020997813441462000,
            "cluster_name": "my-cluster",
            "spark_version": "13.3.x-scala2.12",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 100,
            },
            "custom_tags": {
                "sync:project-id": "b9bd7136-7699-4603-9040-c6dc4c914e43",
                "sync:run-id": "e96401da-f64d-4ed0-8ded-db1317f40248",
                "sync:recommendation-id": "e029a220-c6a5-49fd-b7ed-7ea046366741",
                "sync:tenant-id": "352176a7-b605-4cc2-b3b2-ee591715b6b4",
            },
            "node_type_id": "i6.xlarge",
            "driver_node_type_id": "i6.xlarge",
            "autotermination_minutes": 120,
            "enable_elastic_disk": False,
            "disk_spec": {"disk_count": 0},
            "cluster_source": "UI",
            "enable_local_disk_encryption": False,
            "instance_source": {"node_type_id": "i3.xlarge"},
            "driver_instance_source": {"node_type_id": "i3.xlarge"},
            "spark_conf": {"spark.databricks.isv.product": "sync-gradient"},
            "state": "TERMINATED",
            "state_message": "Inactive cluster terminated (inactive for 120 minutes).",
            "start_time": 1618263108824,
            "terminated_time": 1619746525713,
            "last_state_loss_time": 1619739324740,
            "num_workers": 20,
            "runtime_engine": "PHOTON",
            "default_tags": {
                "Vendor": "Databricks",
                "Creator": "someone@example.com",
                "ClusterName": "my-cluster",
                "ClusterId": "1234-567890-reef123",
            },
            "creator_user_name": "someone@example.com",
            "termination_reason": {
                "code": "INACTIVITY",
                "parameters": {"inactivity_duration_min": "120"},
                "type": "SUCCESS",
            },
            "init_scripts_safe_mode": False,
            "spec": {"spark_version": "13.3.x-scala2.12"},
        }

        with open("tests/test_files/cluster.json") as cluster_in:
            result = test_client.get_updated_cluster_defintion("project_id", cluster_in.read())
            assert result == expected_result
