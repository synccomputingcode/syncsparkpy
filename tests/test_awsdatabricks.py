import copy
import io
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import boto3 as boto
import orjson
from botocore.response import StreamingBody
from botocore.stub import Stubber
from httpx import Response

from sync.awsdatabricks import create_prediction_for_run
from sync.config import DatabricksConf
from sync.models import DatabricksAPIError, DatabricksError

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

MOCK_RUN_WITH_SYNC_TASK = {
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
        },
        {
            "run_id": 5541644,
            "task_key": "sync_task",
            "depends_on": [{"task_key": "load_all_lending_tables_full"}],
            "notebook_task": {
                "notebook_path": "/Users/pete.tamisin@synccomputing.com/sync_task",
                "base_parameters": {
                    "DATABRICKS_RUN_ID": "{{run_id}}",
                    "DATABRICKS_JOB_ID": "{{job_id}}",
                    "DATABRICKS_PARENT_RUN_ID": "{{parent_run_id}}",
                    "DATABRICKS_TASK_KEY": "{{task_key}}",
                },
                "source": "WORKSPACE",
            },
            "job_cluster_key": "Job_cluster",
            "state": {
                "life_cycle_state": "RUNNING",
                "state_message": "In run",
                "user_cancelled_or_timedout": False,
            },
            "run_page_url": "https://dbc-d95d06ca-1d00.cloud.databricks.com/?o=656201176161048#job/1085772780706533/run/5541644",
            "start_time": 1681249788312,
            "setup_duration": 1000,
            "execution_duration": 139000,
            "cleanup_duration": 0,
            "end_time": 1681249828312,
            "cluster_instance": {
                "cluster_id": "0518-173917-zp8ig48r",
                "spark_context_id": "5062870523583018788",
            },
            "attempt_number": 0,
        },
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
        },
        {
            "job_cluster_key": "sync_cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "13.1.x-scala2.12",
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode",
                },
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "us-east-1a",
                    "instance_profile_arn": "arn:aws:iam::471881062455:instance-profile/databricks-workspace-stack-access-data-buckets",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0,
                },
                "node_type_id": "i3.xlarge",
                "custom_tags": {"ResourceClass": "SingleNode"},
                "enable_elastic_disk": True,
                "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                "runtime_engine": "STANDARD",
                "num_workers": 0,
            },
        },
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

MOCK_VOLUMES = {
    "Volumes": [
        {
            "Attachments": [
                {
                    "AttachTime": datetime.fromisoformat("2023-08-15T21:43:03+00:00"),
                    "Device": "/dev/sda1",
                    "InstanceId": "i-01ecbca3d064b99cb",
                    "State": "attached",
                    "VolumeId": "vol-01114146532c3a1c5",
                    "DeleteOnTermination": True,
                }
            ],
            "AvailabilityZone": "us-east-1f",
            "CreateTime": datetime.fromisoformat("2023-08-15T21:43:04.058000+00:00"),
            "Encrypted": True,
            "KmsKeyId": "arn:aws:kms:us-east-1:471881062455:key/75aa19ed-25bd-4fca-be79-c21a23256d69",
            "Size": 30,
            "SnapshotId": "snap-08aeac93ec15d59b0",
            "State": "in-use",
            "VolumeId": "vol-01114146532c3a1c5",
            "Iops": 3000,
            "Tags": [
                {"Key": "ClusterName", "Value": "script-test-nvme"},
                {"Key": "Vendor", "Value": "Databricks"},
                {
                    "Key": "Name",
                    "Value": "workerenv-656201176161048-20de29ca-1c16-42b0-a91e-4f6a30f57313-worker",
                },
                {"Key": "management_service", "Value": "instance_manager_service"},
                {"Key": "ClusterId", "Value": "0815-124039-8cpvy56n"},
                {"Key": "Creator", "Value": "sean.gorsky@synccomputing.com"},
            ],
            "VolumeType": "gp3",
            "MultiAttachEnabled": False,
            "Throughput": 125,
        },
        {
            "Attachments": [
                {
                    "AttachTime": datetime.fromisoformat("2023-08-15T21:43:03+00:00"),
                    "Device": "/dev/xvdb",
                    "InstanceId": "i-01ecbca3d064b99cb",
                    "State": "attached",
                    "VolumeId": "vol-0888642ead4f823ea",
                    "DeleteOnTermination": True,
                }
            ],
            "AvailabilityZone": "us-east-1f",
            "CreateTime": datetime.fromisoformat("2023-08-15T21:43:03.967000+00:00"),
            "Encrypted": True,
            "KmsKeyId": "arn:aws:kms:us-east-1:471881062455:key/75aa19ed-25bd-4fca-be79-c21a23256d69",
            "Size": 150,
            "SnapshotId": "",
            "State": "in-use",
            "VolumeId": "vol-0888642ead4f823ea",
            "Iops": 3000,
            "Tags": [
                {"Key": "ClusterId", "Value": "0815-124039-8cpvy56n"},
                {"Key": "ClusterName", "Value": "script-test-nvme"},
                {"Key": "management_service", "Value": "instance_manager_service"},
                {"Key": "Vendor", "Value": "Databricks"},
                {
                    "Key": "Name",
                    "Value": "workerenv-656201176161048-20de29ca-1c16-42b0-a91e-4f6a30f57313-worker",
                },
                {"Key": "Creator", "Value": "sean.gorsky@synccomputing.com"},
            ],
            "VolumeType": "gp3",
            "MultiAttachEnabled": False,
            "Throughput": 125,
        },
    ]
}

MOCK_DBX_CONF = DatabricksConf(
    host="https://dbc-123.cloud.databricks.com",
    token="my_secret_token",
    aws_region_name="us-east-1",
)


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
def test_create_prediction_for_failed_run(respx_mock):
    failure_response = {"error_code": "FAILED", "message": "This run failed"}

    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=failure_response)
    )

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error
    assert isinstance(result.error, DatabricksAPIError)

    failure_response = {
        "state": {"result_state": "FAILED"},
        "tasks": [
            {
                "task_key": "tpcds_2000GB_group_q76_q80",
                "cluster_instance": {"cluster_id": 12345},
                "state": {"result_state": "FAILED"},
            }
        ],
    }
    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=failure_response)
    )

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error
    assert isinstance(result.error, DatabricksError)


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
def test_create_prediction_for_run_bad_cluster_data(respx_mock):
    # Test too many clusters found
    run_with_multiple_clusters = copy.deepcopy(MOCK_RUN)
    run_with_multiple_clusters["tasks"][0]["cluster_instance"][
        "cluster_id"
    ] = "different_cluster_id"

    run_with_multiple_clusters["tasks"] = [
        MOCK_RUN["tasks"][0],
        run_with_multiple_clusters["tasks"][0],
    ]

    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=run_with_multiple_clusters)
    )

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error

    # Test no tasks/clusters at all
    run_with_no_tasks = copy.deepcopy(MOCK_RUN)
    run_with_no_tasks["tasks"] = []
    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=run_with_no_tasks)
    )

    result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
def test_create_prediction_for_run_no_instances_found(respx_mock):
    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=MOCK_RUN)
    )

    respx_mock.get(
        "https://*.cloud.databricks.com/api/2.0/clusters/get?cluster_id=0101-214342-tpi6qdp2"
    ).mock(return_value=Response(200, json=MOCK_CLUSTER))

    respx_mock.post("https://*.cloud.databricks.com/api/2.0/clusters/events").mock(
        return_value=Response(200, json={"events": [], "total_count": 0})
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    s3 = boto.client("s3")

    s3_stubber = Stubber(s3)
    s3_stubber.add_client_error("get_object", "NoSuchKey")

    def client_patch(name, **kwargs):
        if name == "ec2":
            return ec2
        elif name == "s3":
            return s3

    # First test no instances
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_response("describe_instances", {"Reservations": []})
    ec2_stubber.add_response("describe_volumes", MOCK_VOLUMES)

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
def test_create_prediction_for_run_unauthorized_ec2(respx_mock):
    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=MOCK_RUN)
    )

    respx_mock.get(
        "https://*.cloud.databricks.com/api/2.0/clusters/get?cluster_id=0101-214342-tpi6qdp2"
    ).mock(return_value=Response(200, json=MOCK_CLUSTER))

    respx_mock.post("https://*.cloud.databricks.com/api/2.0/clusters/events").mock(
        return_value=Response(200, json={"events": [], "total_count": 0})
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_client_error(
        "describe_instances",
        service_error_code="AccessDeniedException",
        service_message="User: arn:aws:sts::123456789012:assumed-role/sync-test-no-access/botocore-session-1687389953 is not authorized to perform: elasticmapreduce:DescribeCluster on resource: arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-3GJINYS04BO38 because no identity-based policy allows the elasticmapreduce:DescribeCluster action",
    )

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)
    s3_stubber.add_client_error("get_object", "NoSuchKey")

    def client_patch(name, **kwargs):
        if name == "ec2":
            return ec2
        elif name == "s3":
            return s3

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.error


MOCK_PREDICTION_CREATION_RESPONSE = {
    "result": {
        "prediction_id": str(uuid4()),
        "upload_details": {"url": "https://presigned-url", "fields": {"key": "foobar"}},
    }
}


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
def test_create_prediction_for_run_success(respx_mock):
    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=MOCK_RUN)
    )

    respx_mock.get(
        "https://*.cloud.databricks.com/api/2.0/clusters/get?cluster_id=0101-214342-tpi6qdp2"
    ).mock(return_value=Response(200, json=MOCK_CLUSTER))

    respx_mock.post("https://*.cloud.databricks.com/api/2.0/clusters/events").mock(
        return_value=Response(200, json={"events": [], "total_count": 0})
    )

    respx_mock.post("/v1/auth/token").mock(
        return_value=Response(
            200,
            json={
                "result": {
                    "access_token": "notarealtoken",
                    "expires_at_utc": "2022-09-01T20:54:48Z",
                }
            },
        )
    )

    respx_mock.post("/v1/autotuner/predictions").mock(
        return_value=Response(200, json=MOCK_PREDICTION_CREATION_RESPONSE)
    )

    respx_mock.post(MOCK_PREDICTION_CREATION_RESPONSE["result"]["upload_details"]["url"]).mock(
        return_value=Response(204)
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_response("describe_instances", MOCK_INSTANCES)
    ec2_stubber.add_response("describe_volumes", MOCK_VOLUMES)

    base_prefix = "path/to/logs/0101-214342-tpi6qdp2"
    eventlog_file_prefix = f"{base_prefix}/eventlog/0101-214342-tpi6qdp2"

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)
    s3_stubber.add_client_error("get_object", "NoSuchKey")
    s3_stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": f"{eventlog_file_prefix}/eventlog",
                    "LastModified": datetime.utcfromtimestamp(1681249791560 / 1000),
                }
            ]
        },
        {"Bucket": "bucket", "Prefix": eventlog_file_prefix},
    )
    s3_stubber.add_response(
        "get_object",
        {
            "ContentType": "application/octet-stream",
            "ContentLength": 0,
            "Body": StreamingBody(io.BytesIO(), 0),
        },
        {"Bucket": "bucket", "Key": f"{eventlog_file_prefix}/eventlog"},
    )

    def client_patch(name, **kwargs):
        if name == "ec2":
            return ec2
        elif name == "s3":
            return s3

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.result


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
def test_create_prediction_for_run_success_with_cluster_instance_file(respx_mock):
    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=MOCK_RUN)
    )

    respx_mock.get(
        "https://*.cloud.databricks.com/api/2.0/clusters/get?cluster_id=0101-214342-tpi6qdp2"
    ).mock(return_value=Response(200, json=MOCK_CLUSTER))

    respx_mock.post("https://*.cloud.databricks.com/api/2.0/clusters/events").mock(
        return_value=Response(200, json={"events": [], "total_count": 0})
    )

    respx_mock.post("/v1/auth/token").mock(
        return_value=Response(
            200,
            json={
                "result": {
                    "access_token": "notarealtoken",
                    "expires_at_utc": "2022-09-01T20:54:48Z",
                }
            },
        )
    )

    respx_mock.post("/v1/autotuner/predictions").mock(
        return_value=Response(200, json=MOCK_PREDICTION_CREATION_RESPONSE)
    )

    respx_mock.post(MOCK_PREDICTION_CREATION_RESPONSE["result"]["upload_details"]["url"]).mock(
        return_value=Response(204)
    )

    base_prefix = "path/to/logs/0101-214342-tpi6qdp2"
    eventlog_file_prefix = f"{base_prefix}/eventlog/0101-214342-tpi6qdp2"
    cluster_info_file_key = f"{base_prefix}/sync_data/1443449481634833945/aws_cluster_info.json"

    # Don't add any responses for this one as we expect all the instance data we need to be available
    #  in the cluster_instances.json file
    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)

    mock_cluster_info_bytes = orjson.dumps(
        {**MOCK_INSTANCES, **MOCK_VOLUMES},
        option=orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_NAIVE_UTC,
    )
    s3_stubber.add_response(
        "get_object",
        {
            "ContentType": "application/octet-stream",
            "ContentLength": len(mock_cluster_info_bytes),
            "Body": StreamingBody(
                io.BytesIO(mock_cluster_info_bytes),
                len(mock_cluster_info_bytes),
            ),
        },
        {"Bucket": "bucket", "Key": cluster_info_file_key},
    )
    s3_stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": f"{eventlog_file_prefix}/eventlog",
                    "LastModified": datetime.utcfromtimestamp(1681249791560 / 1000),
                }
            ]
        },
        {"Bucket": "bucket", "Prefix": eventlog_file_prefix},
    )
    s3_stubber.add_response(
        "get_object",
        {
            "ContentType": "application/octet-stream",
            "ContentLength": 0,
            "Body": StreamingBody(io.BytesIO(), 0),
        },
        {"Bucket": "bucket", "Key": f"{eventlog_file_prefix}/eventlog"},
    )

    def client_patch(name, **kwargs):
        if name == "ec2":
            return ec2
        elif name == "s3":
            return s3

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.result


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
def test_create_prediction_for_run_with_pending_task(respx_mock):
    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=MOCK_RUN_WITH_SYNC_TASK)
    )

    respx_mock.get(
        "https://*.cloud.databricks.com/api/2.0/clusters/get?cluster_id=0101-214342-tpi6qdp2"
    ).mock(return_value=Response(200, json=MOCK_CLUSTER))

    respx_mock.post("https://*.cloud.databricks.com/api/2.0/clusters/events").mock(
        return_value=Response(200, json={"events": [], "total_count": 0})
    )

    respx_mock.post("/v1/auth/token").mock(
        return_value=Response(
            200,
            json={
                "result": {
                    "access_token": "notarealtoken",
                    "expires_at_utc": "2022-09-01T20:54:48Z",
                }
            },
        )
    )

    respx_mock.post("/v1/autotuner/predictions").mock(
        return_value=Response(200, json=MOCK_PREDICTION_CREATION_RESPONSE)
    )

    respx_mock.post(MOCK_PREDICTION_CREATION_RESPONSE["result"]["upload_details"]["url"]).mock(
        return_value=Response(204)
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_response("describe_instances", MOCK_INSTANCES)
    ec2_stubber.add_response("describe_volumes", MOCK_VOLUMES)

    s3_file_prefix = "path/to/logs/0101-214342-tpi6qdp2/eventlog/0101-214342-tpi6qdp2"

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)
    s3_stubber.add_client_error("get_object", "NoSuchKey")
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
        if name == "ec2":
            return ec2
        elif name == "s3":
            return s3

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run(
            "75778", "Premium", "Jobs Compute", "my-project-id", exclude_tasks=["sync_task"]
        )

    assert result.result


@patch("sync.awsdatabricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.clients.databricks.DB_CONFIG", new=MOCK_DBX_CONF)
@patch("sync.awsdatabricks.event_log_poll_duration_seconds")
def test_create_prediction_for_run_event_log_upload_delay(
    event_log_poll_duration_seconds, respx_mock
):
    event_log_poll_duration_seconds.return_value = 0

    respx_mock.get("https://*.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=75778").mock(
        return_value=Response(200, json=MOCK_RUN)
    )

    respx_mock.get(
        "https://*.cloud.databricks.com/api/2.0/clusters/get?cluster_id=0101-214342-tpi6qdp2"
    ).mock(return_value=Response(200, json=MOCK_CLUSTER))

    respx_mock.post("https://*.cloud.databricks.com/api/2.0/clusters/events").mock(
        return_value=Response(200, json={"events": [], "total_count": 0})
    )

    respx_mock.post("/v1/auth/token").mock(
        return_value=Response(
            200,
            json={
                "result": {
                    "access_token": "notarealtoken",
                    "expires_at_utc": "2022-09-01T20:54:48Z",
                }
            },
        )
    )

    respx_mock.post("/v1/autotuner/predictions").mock(
        return_value=Response(200, json=MOCK_PREDICTION_CREATION_RESPONSE)
    )

    respx_mock.post(MOCK_PREDICTION_CREATION_RESPONSE["result"]["upload_details"]["url"]).mock(
        return_value=Response(204)
    )

    ec2 = boto.client("ec2", region_name=MOCK_DBX_CONF.aws_region_name)
    ec2_stubber = Stubber(ec2)
    ec2_stubber.add_response("describe_instances", MOCK_INSTANCES)
    ec2_stubber.add_response("describe_volumes", MOCK_VOLUMES)

    s3_file_prefix = "path/to/logs/0101-214342-tpi6qdp2/eventlog/0101-214342-tpi6qdp2"

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)
    s3_stubber.add_client_error("get_object", "NoSuchKey")

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
        if name == "ec2":
            return ec2
        elif name == "s3":
            return s3

    with s3_stubber, ec2_stubber, patch("boto3.client") as mock_aws_client:
        mock_aws_client.side_effect = client_patch
        result = create_prediction_for_run("75778", "Premium", "Jobs Compute", "my-project-id")

    assert result.result
