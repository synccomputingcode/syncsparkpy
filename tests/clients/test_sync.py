import json
from unittest import TestCase, mock

from sync.api.projects import (
    get_cluster_definition_and_recommendation,
    get_latest_project_config_recommendation,
    get_updated_cluster_definition,
)
from sync.models import RecommendationError, Response


def get_aws_rec_from_file():
    with open("tests/test_files/aws_recommendation.json") as rec_in:
        return json.loads(rec_in.read())


def get_azure_rec_from_file():
    with open("tests/test_files/azure_recommendation.json") as rec_in:
        return json.loads(rec_in.read())


class TestSync(TestCase):
    @mock.patch("sync.clients.sync.SyncClient._send")
    def test_get_latest_aws_project_config_recommendation(self, mock_send):
        mock_send.return_value = get_aws_rec_from_file()
        expected_result = Response(
            result={
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
        )

        result = get_latest_project_config_recommendation("project_id")
        assert result == expected_result

    @mock.patch("sync.clients.sync.SyncClient._send")
    def test_get_latest_azure_project_config_recommendation(self, mock_send):
        mock_send.return_value = get_azure_rec_from_file()
        expected_result = Response(
            result={
                "node_type_id": "Standard_D4s_v3",
                "driver_node_type_id": "Standard_D4s_v3",
                "custom_tags": {
                    "sync:project-id": "769c3443-afd7-45ff-a72a-27bf4296b80e",
                    "sync:run-id": "d3f8db6c-df4b-430a-a511-a1e9c95d1ad0",
                    "sync:recommendation-id": "6024acdd-fd13-4bf1-82f5-44f1ab7008f2",
                    "sync:tenant-id": "290d381e-8eb4-4d6a-80d4-453d82897ecc",
                },
                "num_workers": 5,
                "spark_conf": {"spark.databricks.isv.product": "sync-gradient"},
                "spark_version": "13.3.x-scala2.12",
                "runtime_engine": "STANDARD",
                "azure_attributes": {
                    "availability": "SPOT_WITH_FALLBACK_AZURE",
                    "first_on_demand": 7,
                    "spot_bid_max_price": 100.0,
                },
            }
        )

        result = get_latest_project_config_recommendation("project_id")
        assert result == expected_result

    @mock.patch("sync.clients.sync.SyncClient._send")
    def test_get_cluster_definition_no_recommendation(self, mock_send):
        mock_send.return_value = {"result": []}
        expected_result = Response(error=RecommendationError(message="Recommendation failed"))

        with open("tests/test_files/azure_cluster.json") as cluster_in:
            result = get_cluster_definition_and_recommendation("project_id", cluster_in.read())
            assert result == expected_result

    @mock.patch("sync.clients.sync.SyncClient._send")
    def test_get_cluster_definition_and_recommendation(self, mock_send):
        mock_send.return_value = get_aws_rec_from_file()

        expected_result = Response(
            result={
                "cluster_recommendation": {
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
        )

        with open("tests/test_files/aws_cluster.json") as cluster_in:
            result = get_cluster_definition_and_recommendation("project_id", cluster_in.read())
            assert result == expected_result

    @mock.patch("sync.clients.sync.SyncClient.get_latest_project_recommendation")
    def test_get_updated_aws_cluster_definition(self, mock_send):
        mock_send.return_value = get_aws_rec_from_file()

        expected_result = Response(
            result={
                "cluster_id": "1234-567890-reef123",
                "spark_context_id": 4020997813441462000,
                "cluster_name": "my-cluster",
                "spark_version": "13.3.x-scala2.12",
                "aws_attributes": {
                    "first_on_demand": 1,
                    "ebs_volume_count": 0,
                    "availability": "SPOT_WITH_FALLBACK",
                    "spot_bid_price_percent": 100,
                    "zone_id": "us-west-2c",
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
        )

        with open("tests/test_files/aws_cluster.json") as cluster_in:
            result = get_updated_cluster_definition("project_id", cluster_in.read())
            assert result == expected_result

    @mock.patch("sync.clients.sync.SyncClient.get_latest_project_recommendation")
    def test_get_updated_azure_cluster_definition(self, mock_send):
        mock_send.return_value = get_azure_rec_from_file()

        expected_result = Response(
            result={
                "cluster_id": "1114-202840-mu1ql9xp",
                "spark_context_id": 8637481617925571639,
                "cluster_name": "my-cluster",
                "spark_version": "13.3.x-scala2.12",
                "azure_attributes": {
                    "first_on_demand": 7,
                    "availability": "SPOT_WITH_FALLBACK_AZURE",
                    "spot_bid_max_price": 100.0,
                },
                "node_type_id": "Standard_D4s_v3",
                "driver_node_type_id": "Standard_D4s_v3",
                "autotermination_minutes": 120,
                "enable_elastic_disk": False,
                "disk_spec": {"disk_count": 0},
                "cluster_source": "UI",
                "enable_local_disk_encryption": False,
                "instance_source": {"node_type_id": "Standard_DS5_v2"},
                "driver_instance_source": {"node_type_id": "Standard_DS5_v2"},
                "state": "TERMINATED",
                "state_message": "Inactive cluster terminated (inactive for 120 minutes).",
                "start_time": 1618263108824,
                "terminated_time": 1619746525713,
                "last_state_loss_time": 1619739324740,
                "num_workers": 5,
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
                "custom_tags": {
                    "sync:project-id": "769c3443-afd7-45ff-a72a-27bf4296b80e",
                    "sync:run-id": "d3f8db6c-df4b-430a-a511-a1e9c95d1ad0",
                    "sync:recommendation-id": "6024acdd-fd13-4bf1-82f5-44f1ab7008f2",
                    "sync:tenant-id": "290d381e-8eb4-4d6a-80d4-453d82897ecc",
                },
                "spark_conf": {"spark.databricks.isv.product": "sync-gradient"},
                "runtime_engine": "STANDARD",
            }
        )

        with open("tests/test_files/azure_cluster.json") as cluster_in:
            result = get_updated_cluster_definition("project_id", cluster_in.read())
            assert result == expected_result
