import unittest
from unittest.mock import patch

from sync.awsdatabricks import monitor_cluster
from sync.config import DatabricksConf

MOCK_DBX_CONF = DatabricksConf(
    host="https://dbc-123.cloud.databricks.com",
    token="my_secret_token",
    aws_region_name="us-east-1",
)


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.awsdatabricks._monitor_cluster")
@patch("sync.clients.databricks.DatabricksClient.get_cluster")
@patch("sync.awsdatabricks._cluster_log_destination")
class TestMonitorCluster(unittest.TestCase):
    def test_monitor_cluster_with_override(
        self,
        mock_cluster_log_destination,
        mock_get_cluster,
        mock_monitor_cluster,
    ):
        mock_cluster_log_destination.return_value = ("s3://bucket/path", "s3", "bucket", "path")

        mock_get_cluster.return_value = {
            "cluster_id": "0101-214342-tpi6qdp2",
            "spark_context_id": 1443449481634833945,
        }

        cluster_report_destination_override = {
            "filesystem": "file",
            "base_prefix": "test_file_path",
        }

        monitor_cluster("0101-214342-tpi6qdp2", 1, cluster_report_destination_override, True)

        expected_log_destination_override = ("s3://bucket/path", "file", "bucket", "test_file_path")
        mock_monitor_cluster.assert_called_with(
            expected_log_destination_override, "0101-214342-tpi6qdp2", 1443449481634833945, 1, True
        )

        mock_cluster_log_destination.return_value = (None, "s3", None, "path")
        monitor_cluster("0101-214342-tpi6qdp2", 1, cluster_report_destination_override, True)
        expected_log_destination_override = (None, "file", None, "test_file_path")
        mock_monitor_cluster.assert_called_with(
            expected_log_destination_override, "0101-214342-tpi6qdp2", 1443449481634833945, 1, True
        )

    def test_monitor_cluster_without_override(
        self,
        mock_cluster_log_destination,
        mock_get_cluster,
        mock_monitor_cluster,
    ):
        mock_cluster_log_destination.return_value = ("s3://bucket/path", "s3", "bucket", "path")

        mock_get_cluster.return_value = {
            "cluster_id": "0101-214342-tpi6qdp2",
            "spark_context_id": 1443449481634833945,
        }

        monitor_cluster("0101-214342-tpi6qdp2", 1)

        mock_monitor_cluster.assert_called_with(
            mock_cluster_log_destination.return_value,
            "0101-214342-tpi6qdp2",
            1443449481634833945,
            1,
            False,
        )
