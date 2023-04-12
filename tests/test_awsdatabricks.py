import copy
import io
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import boto3 as boto
from botocore.response import StreamingBody
from botocore.stub import Stubber

from sync.awsdatabricks import create_prediction_for_run
from sync.config import DatabricksConf
from sync.models import Response

MOCK_RUN = {
    "job_id": 12345678910,
    "run_id": 75778,
    "creator_user_name": "user_name@domain.com",
    "number_in_job": 75778,
    "original_attempt_run_id": 75778,
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "SUCCESS",
        "state_message": "",
        "user_cancelled_or_timedout": False,
    },
    "start_time": 1681249421062,
    "setup_duration": 237000,
    "execution_duration": 130000,
    "cleanup_duration": 0,
    "end_time": 1681249788433,
    "trigger": "ONE_TIME",
    "run_name": "test_job",
    "run_page_url": "https://dbc-foo-bar.cloud.databricks.com/?o=12345678910#job/10987654321/run/12345",
    "run_type": "JOB_RUN",
    "tasks": [
        {
            "run_id": 76722,
            "task_key": "my_task",
            "notebook_task": {"notebook_path": "/Users/user/notebook", "source": "WORKSPACE"},
            "job_cluster_key": "my_job_cluster",
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False,
            },
            "run_page_url": "https://dbc-foo-bar.cloud.databricks.com/?o=12345678910#job/10987654321/run/12345",
            "start_time": 1681249421074,
            "setup_duration": 237000,
            "execution_duration": 130000,
            "cleanup_duration": 0,
            "end_time": 1681249788312,
            "cluster_instance": {
                "cluster_id": "0101-214342-tpi6qdp2",
                "spark_context_id": "1443449481634833945",
            },
            "attempt_number": 0,
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "my_job_cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "12.2.x-scala2.12",
                "aws_attributes": {
                    "first_on_demand": 2,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "auto",
                    "instance_profile_arn": "arn:aws:iam::123456789:instance-profile/my-iam-profile",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0,
                },
                "node_type_id": "i3.4xlarge",
                "driver_node_type_id": "i3.xlarge",
                "cluster_log_conf": {
                    "s3": {
                        "destination": "s3://bucket/path/to/logs/",
                        "region": "us-east-1",
                        "enable_encryption": True,
                        "canned_acl": "bucket-owner-full-control",
                    }
                },
                "enable_elastic_disk": True,
                "policy_id": "9C6308F703005DF2",
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "PHOTON",
                "num_workers": 1,
            },
        }
    ],
    "format": "MULTI_TASK",
}

MOCK_CLUSTER = {
    "cluster_id": "0101-214342-tpi6qdp2",
    "creator_user_name": "user_name@domain.com",
    "spark_context_id": 1443449481634833945,
    "driver_healthy": True,
    "cluster_name": "job-12345678910-run-75778-my_job_run",
    "spark_version": "12.2.x-scala2.12",
    "aws_attributes": {
        "first_on_demand": 2,
        "availability": "SPOT_WITH_FALLBACK",
        "zone_id": "auto",
        "instance_profile_arn": "arn:aws:iam::123456789:instance-profile/my-iam-profile",
        "spot_bid_price_percent": 100,
        "ebs_volume_count": 0,
    },
    "node_type_id": "i3.4xlarge",
    "driver_node_type_id": "i3.xlarge",
    "cluster_log_conf": {
        "s3": {
            "destination": "s3://bucket/path/to/logs/",
            "region": "us-east-1",
            "enable_encryption": True,
            "canned_acl": "bucket-owner-full-control",
        }
    },
    "autotermination_minutes": 0,
    "enable_elastic_disk": True,
    "disk_spec": {"disk_count": 0},
    "cluster_source": "JOB",
    "single_user_name": "user_name@domain.com",
    "policy_id": "9C6308F703005DF2",
    "enable_local_disk_encryption": False,
    "instance_source": {"node_type_id": "i3.4xlarge"},
    "driver_instance_source": {"node_type_id": "i3.xlarge"},
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "PHOTON",
    "effective_spark_version": "12.2.x-photon-scala2.12",
    "state": "TERMINATED",
    "state_message": "",
    "start_time": 1681249423048,
    "terminated_time": 1681249791560,
    "last_state_loss_time": 0,
    "last_activity_time": 1681249606510,
    "last_restarted_time": 1681249654358,
    "num_workers": 1,
    "default_tags": {
        "Vendor": "Databricks",
        "Creator": "user_name@domain.com",
        "ClusterName": "job-12345678910-run-75778-my_job_run",
        "ClusterId": "0101-214342-tpi6qdp2",
        "JobId": "943021334099449",
        "RunName": "my_job_run",
    },
    "cluster_log_status": {"last_attempted": 1681249693295},
    "termination_reason": {"code": "JOB_FINISHED", "type": "SUCCESS"},
    "init_scripts_safe_mode": False,
}

MOCK_INSTANCES = {
    "Reservations": [
        {
            "Instances": [
                {
                    "AmiLaunchIndex": 0,
                    "ImageId": "ami-0ea2c19e79de11215",
                    "InstanceId": "i-0ae24904bc2811797",
                    "InstanceType": "i3.xlarge",
                    "LaunchTime": datetime.fromisoformat("2023-04-04T22:51:24+00:00"),
                    "Monitoring": {
                        "State": "disabled",
                    },
                    "Placement": {
                        "AvailabilityZone": "us-east-1c",
                        "GroupName": "",
                        "Tenancy": "default",
                    },
                    "PrivateDnsName": "",
                    "ProductCodes": [],
                    "PublicDnsName": "",
                    "State": {"Code": 48, "Name": "terminated"},
                    "Architecture": "x86_64",
                    "BlockDeviceMappings": [],
                    "ClientToken": "f05ee9c8-a720-4e60-ace7-cd188da120ea",
                    "EbsOptimized": False,
                    "EnaSupport": True,
                    "Hypervisor": "xen",
                    "InstanceLifecycle": "spot",
                    "NetworkInterfaces": [],
                    "RootDeviceName": "/dev/sda1",
                    "RootDeviceType": "ebs",
                    "SecurityGroups": [],
                    "SpotInstanceRequestId": "sir-mzqyjsdg",
                    "StateReason": {
                        "Code": "Client.UserInitiatedShutdown",
                        "Message": "Client.UserInitiatedShutdown: User initiated shutdown",
                    },
                    "Tags": [
                        {"Key": "RunName", "Value": "my_test_job"},
                        {"Key": "Vendor", "Value": "Databricks"},
                        {"Key": "management_service", "Value": "instance_manager_service"},
                        {"Key": "ClusterId", "Value": "0101-214342-tpi6qdp2"},
                        {"Key": "Creator", "Value": "user@domain.com"},
                        {"Key": "JobId", "Value": "943021334099449"},
                        {
                            "Key": "Name",
                            "Value": "workerenv-1187965937856149-f0bf0016-45bf-496b-9f05-16cf93ffb24d-worker",
                        },
                        {
                            "Key": "ClusterName",
                            "Value": "job-12345678910-run-75778-my_job_run",
                        },
                    ],
                    "VirtualizationType": "hvm",
                    "CpuOptions": {"CoreCount": 2, "ThreadsPerCore": 2},
                    "CapacityReservationSpecification": {"CapacityReservationPreference": "open"},
                    "HibernationOptions": {"Configured": False},
                    "MetadataOptions": {
                        "State": "pending",
                        "HttpTokens": "required",
                        "HttpPutResponseHopLimit": 2,
                        "HttpEndpoint": "enabled",
                        "HttpProtocolIpv6": "disabled",
                        "InstanceMetadataTags": "disabled",
                    },
                    "EnclaveOptions": {"Enabled": False},
                    "PlatformDetails": "Linux/UNIX",
                    "UsageOperation": "RunInstances",
                    "UsageOperationUpdateTime": datetime.fromisoformat("2023-04-04T22:51:24+00:00"),
                    "MaintenanceOptions": {"AutoRecovery": "default"},
                }
            ],
        },
        {
            "Instances": [
                {
                    "AmiLaunchIndex": 0,
                    "ImageId": "ami-0ea2c19e79de11215",
                    "InstanceId": "i-0ae24904bc2811797",
                    "InstanceType": "i3.4xlarge",
                    "LaunchTime": datetime.fromisoformat("2023-04-04T22:51:24+00:00"),
                    "Monitoring": {
                        "State": "disabled",
                    },
                    "Placement": {
                        "AvailabilityZone": "us-east-1c",
                        "GroupName": "",
                        "Tenancy": "default",
                    },
                    "PrivateDnsName": "",
                    "ProductCodes": [],
                    "PublicDnsName": "",
                    "State": {"Code": 48, "Name": "terminated"},
                    "Architecture": "x86_64",
                    "BlockDeviceMappings": [],
                    "ClientToken": "f05ee9c8-a720-4e60-ace7-cd188da120ea",
                    "EbsOptimized": False,
                    "EnaSupport": True,
                    "Hypervisor": "xen",
                    "InstanceLifecycle": "spot",
                    "NetworkInterfaces": [],
                    "RootDeviceName": "/dev/sda1",
                    "RootDeviceType": "ebs",
                    "SecurityGroups": [],
                    "SpotInstanceRequestId": "sir-mzqyjsdg",
                    "StateReason": {
                        "Code": "Client.UserInitiatedShutdown",
                        "Message": "Client.UserInitiatedShutdown: User initiated shutdown",
                    },
                    "Tags": [
                        {"Key": "RunName", "Value": "my_test_job"},
                        {"Key": "Vendor", "Value": "Databricks"},
                        {"Key": "management_service", "Value": "instance_manager_service"},
                        {"Key": "ClusterId", "Value": "0101-214342-tpi6qdp2"},
                        {"Key": "Creator", "Value": "user@domain.com"},
                        {"Key": "JobId", "Value": "943021334099449"},
                        {
                            "Key": "Name",
                            "Value": "workerenv-1187965937856149-f0bf0016-45bf-496b-9f05-16cf93ffb24d-worker",
                        },
                        {
                            "Key": "ClusterName",
                            "Value": "job-12345678910-run-75778-my_job_run",
                        },
                    ],
                    "VirtualizationType": "hvm",
                    "CpuOptions": {"CoreCount": 8, "ThreadsPerCore": 8},
                    "CapacityReservationSpecification": {"CapacityReservationPreference": "open"},
                    "HibernationOptions": {"Configured": False},
                    "MetadataOptions": {
                        "State": "pending",
                        "HttpTokens": "required",
                        "HttpPutResponseHopLimit": 2,
                        "HttpEndpoint": "enabled",
                        "HttpProtocolIpv6": "disabled",
                        "InstanceMetadataTags": "disabled",
                    },
                    "EnclaveOptions": {"Enabled": False},
                    "PlatformDetails": "Linux/UNIX",
                    "UsageOperation": "RunInstances",
                    "UsageOperationUpdateTime": datetime.fromisoformat("2023-04-04T22:51:24+00:00"),
                    "MaintenanceOptions": {"AutoRecovery": "default"},
                }
            ]
        },
    ],
}


class MockDatabricksClient:
    def __init__(self, responses):
        self._responses = responses
        return

    def get_run(self, run_id: str):
        return self._responses["get_run"][run_id]

    def get_cluster(self, cluster_id: str):
        return self._responses["get_cluster"][cluster_id]

    def get_cluster_events(self, cluster_id: str):
        return self._responses["get_cluster_events"][cluster_id]


MOCK_DBX_CONF = DatabricksConf(
    host="https://dbc-foo-bar.cloud.databricks.com/",
    token="dbcmytoken",
    aws_region_name="us-east-1",
)


@patch("sync.clients.databricks.get_default_client")
def test_create_prediction_for_run_failed_run(get_default_client):
    failure_response = {"error_code": "FAILED", "message": "This run failed"}
    mock_dbx_client = MockDatabricksClient(
        {
            "get_run": {"75778": failure_response},
        }
    )
    get_default_client.return_value = mock_dbx_client

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error
    assert result.error.message.index(failure_response["message"])

    failure_response = {"state": {"result_state": "FAILED"}}
    mock_dbx_client = MockDatabricksClient(
        {
            "get_run": {"75778": failure_response},
        }
    )
    get_default_client.return_value = mock_dbx_client

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error


@patch("sync.clients.databricks.get_databricks_config")
@patch("sync.clients.databricks.get_default_client")
def test_create_prediction_for_run_bad_cluster_data(get_default_client, get_databricks_config):
    get_databricks_config.return_value = MOCK_DBX_CONF

    # Test too many clusters found
    run_with_multiple_clusters = copy.deepcopy(MOCK_RUN)
    run_with_multiple_clusters["tasks"][0]["cluster_instance"][
        "cluster_id"
    ] = "different_cluster_id"

    run_with_multiple_clusters["tasks"] = [
        MOCK_RUN["tasks"][0],
        run_with_multiple_clusters["tasks"][0],
    ]

    mock_dbx_client = MockDatabricksClient({"get_run": {"75778": run_with_multiple_clusters}})
    get_default_client.return_value = mock_dbx_client

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error

    # Test no tasks/clusters at all
    run_with_no_tasks = copy.deepcopy(MOCK_RUN)
    run_with_no_tasks["tasks"] = []
    mock_dbx_client = MockDatabricksClient({"get_run": {"75778": run_with_no_tasks}})
    get_default_client.return_value = mock_dbx_client

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error


@patch("sync.clients.databricks.get_databricks_config")
@patch("sync.clients.databricks.get_default_client")
def test_create_prediction_for_run_no_instances_found(get_default_client, get_databricks_config):
    get_databricks_config.return_value = MOCK_DBX_CONF

    get_default_client.return_value = MockDatabricksClient(
        {
            "get_run": {"75778": MOCK_RUN},
            "get_cluster": {"0101-214342-tpi6qdp2": MOCK_CLUSTER},
            "get_cluster_events": {"0101-214342-tpi6qdp2": {"events": [], "total_count": 0}},
        }
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_response("describe_instances", {"Reservations": []})

    def client_patch(name, **kwargs):
        match name:
            case "ec2":
                return ec2

    with ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error


@patch("sync.clients.databricks.get_databricks_config")
@patch("sync.clients.databricks.get_default_client")
@patch("sync.awsdatabricks.create_prediction")
def test_create_prediction_for_run_success(
    create_prediction, get_default_client, get_databricks_config
):
    create_prediction.return_value = Response(result=uuid4())

    get_databricks_config.return_value = MOCK_DBX_CONF

    get_default_client.return_value = MockDatabricksClient(
        {
            "get_run": {"75778": MOCK_RUN},
            "get_cluster": {"0101-214342-tpi6qdp2": MOCK_CLUSTER},
            "get_cluster_events": {"0101-214342-tpi6qdp2": {"events": [], "total_count": 0}},
        }
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_response("describe_instances", MOCK_INSTANCES)

    s3_file_prefix = "path/to/logs/0101-214342-tpi6qdp2/eventlog/0101-214342-tpi6qdp2"

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)
    s3_stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": f"{s3_file_prefix}/eventlog",
                    "LastModified": datetime.utcfromtimestamp(1681249791560 / 1000),
                }
            ]
        },
        {"Bucket": "bucket", "Prefix": s3_file_prefix},
    )
    s3_stubber.add_response(
        "get_object",
        {
            "ContentType": "application/octet-stream",
            "ContentLength": 0,
            "Body": StreamingBody(io.BytesIO(), 0),
        },
        {"Bucket": "bucket", "Key": f"{s3_file_prefix}/eventlog"},
    )

    def client_patch(name, **kwargs):
        match name:
            case "s3":
                return s3
            case "ec2":
                return ec2

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert not result.error
    assert result.result


@patch("sync.clients.databricks.get_databricks_config")
@patch("sync.clients.databricks.get_default_client")
@patch("sync.awsdatabricks.event_log_poll_duration_seconds")
@patch("sync.awsdatabricks.create_prediction")
def test_create_prediction_for_run_event_log_upload_delay(
    create_prediction, event_log_poll_duration_seconds, get_default_client, get_databricks_config
):
    create_prediction.return_value = Response(result=uuid4())

    event_log_poll_duration_seconds.return_value = 0

    get_databricks_config.return_value = MOCK_DBX_CONF

    get_default_client.return_value = MockDatabricksClient(
        {
            "get_run": {"75778": MOCK_RUN},
            "get_cluster": {"0101-214342-tpi6qdp2": MOCK_CLUSTER},
            "get_cluster_events": {"0101-214342-tpi6qdp2": {"events": [], "total_count": 0}},
        }
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_response("describe_instances", MOCK_INSTANCES)

    s3_file_prefix = "path/to/logs/0101-214342-tpi6qdp2/eventlog/0101-214342-tpi6qdp2"

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)

    # Test no event log files present yet
    s3_stubber.add_response(
        "list_objects_v2", {"Contents": []}, {"Bucket": "bucket", "Prefix": s3_file_prefix}
    )

    # Test incomplete event log data present
    s3_stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": f"{s3_file_prefix}/eventlog-2023-04-11--23-30.gz",
                    "LastModified": datetime.utcfromtimestamp(1681249688400 / 1000),
                }
            ]
        },
        {"Bucket": "bucket", "Prefix": s3_file_prefix},
    )

    # Test still waiting for remaining data to make it to the final event log file
    s3_stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": f"{s3_file_prefix}/eventlog",
                    "LastModified": datetime.fromtimestamp(1681249688433 / 1000),
                },
                {
                    "Key": f"{s3_file_prefix}/eventlog-2023-04-11--23-30.gz",
                    "LastModified": datetime.fromtimestamp(1681249588400 / 1000),
                },
            ]
        },
        {"Bucket": "bucket", "Prefix": s3_file_prefix},
    )

    # Finally, all the data is present
    s3_stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": f"{s3_file_prefix}/eventlog",
                    "LastModified": datetime.fromtimestamp(1681249788435 / 1000),
                },
                {
                    "Key": f"{s3_file_prefix}/eventlog-2023-04-11--23-30.gz",
                    "LastModified": datetime.fromtimestamp(1681249588400 / 1000),
                },
            ]
        },
        {"Bucket": "bucket", "Prefix": s3_file_prefix},
    )

    s3_stubber.add_response(
        "get_object",
        {
            "ContentType": "application/octet-stream",
            "ContentLength": 0,
            "Body": StreamingBody(io.BytesIO(), 0),
        },
        {"Bucket": "bucket", "Key": f"{s3_file_prefix}/eventlog"},
    )

    s3_stubber.add_response(
        "get_object",
        {
            "ContentType": "application/octet-stream",
            "ContentLength": 0,
            "Body": StreamingBody(io.BytesIO(), 0),
        },
        {"Bucket": "bucket", "Key": f"{s3_file_prefix}/eventlog-2023-04-11--23-30.gz"},
    )

    def client_patch(name, **kwargs):
        match name:
            case "s3":
                return s3
            case "ec2":
                return ec2

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert not result.error
    assert result.result
