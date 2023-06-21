from unittest.mock import Mock, patch

import boto3 as boto
import orjson
from botocore.stub import ANY, Stubber
from dateutil.parser import parse
from deepdiff import DeepDiff

from sync import TIME_FORMAT
from sync.awsemr import (
    create_prediction_for_cluster,
    get_cluster_report,
    get_project_cluster_report,
)
from sync.models import Response


@patch("sync.awsemr.get_cluster_report")
@patch("sync.awsemr.create_prediction")
def test_create_prediction(create_prediction, get_cluster_report):
    with open("tests/data/emr-cluster-report.json") as emr_cluster_report_fobj:
        get_cluster_report.return_value = Response(
            result=orjson.loads(emr_cluster_report_fobj.read())
        )

    prediction_id = "320554b0-3972-4b7c-9e41-c8efdbdc042c"
    create_prediction.return_value = Response(result=prediction_id)

    s3 = boto.client("s3")
    stubber = Stubber(s3)

    stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": "29f4dded-70be-4344-b9b5-396c8c0481cf/2023-03-07T04:14:28Z/f84639ed-7a6a-4496-81e1-b5ba8fa8b6ce/eventlog/application_1678162862227_0001"
                }
            ]
        },
        {
            "Bucket": "my-emr-projects",
            "Prefix": "29f4dded-70be-4344-b9b5-396c8c0481cf/2023-03-07T04:14:28Z/f84639ed-7a6a-4496-81e1-b5ba8fa8b6ce/eventlog/",
        },
    )

    s3_mock = Mock(wraps=s3)

    # This method cannot be stubbed like list_objects_v2
    s3_mock.generate_presigned_url.return_value = (
        "https://my-emr-projects.s3.amazonaws.com/something/something"
    )

    with stubber, patch("boto3.client") as mock_client:
        mock_client.return_value = s3_mock
        response = create_prediction_for_cluster(
            get_cluster_report.return_value.result["Cluster"]["Id"]
        )

    assert prediction_id == response.result


def test_get_cluster_report():
    with open("tests/data/emr-cluster-report.json") as emr_cluster_report_fobj:
        emr_cluster_report = orjson.loads(emr_cluster_report_fobj.read())

    cluster_id = emr_cluster_report["Cluster"]["Id"]
    region = emr_cluster_report["Region"]

    emr = boto.client("emr")
    stubber = Stubber(emr)

    describe_response = {"Cluster": emr_cluster_report["Cluster"].copy()}
    del describe_response["Cluster"]["BootstrapActions"]
    del describe_response["Cluster"]["InstanceFleets"]

    stubber.add_response("describe_cluster", describe_response, {"ClusterId": cluster_id})
    stubber.add_response(
        "list_bootstrap_actions",
        {"BootstrapActions": emr_cluster_report["Cluster"]["BootstrapActions"]},
        {"ClusterId": cluster_id},
    )
    stubber.add_response(
        "list_instance_fleets",
        {"InstanceFleets": emr_cluster_report["Cluster"]["InstanceFleets"]},
        {"ClusterId": cluster_id},
    )
    stubber.add_response(
        "list_instances", {"Instances": emr_cluster_report["Instances"]}, {"ClusterId": cluster_id}
    )
    stubber.add_response(
        "list_steps", {"Steps": emr_cluster_report["Steps"]}, {"ClusterId": cluster_id}
    )

    with stubber, patch("boto3.client") as mock_client:
        mock_client.return_value = emr
        result = get_cluster_report(cluster_id, region).result

    assert not DeepDiff(emr_cluster_report, result)


@patch("sync.awsemr.get_cluster_report")
@patch("sync.awsemr.get_project")
def test_get_project_report(get_project, get_cluster_report):
    with open("tests/data/emr-cluster-report.json") as emr_cluster_report_fobj:
        cluster_report = orjson.loads(emr_cluster_report_fobj.read())
        get_cluster_report.return_value = Response(result=cluster_report)

    get_project.return_value = Response(
        result={
            "created_at": "2023-01-20T00:38:10Z",
            "updated_at": "2023-03-10T17:18:50Z",
            "id": "4f5fe783-df74-4d64-adad-a635d6319579",
            "name": "Data Insights",
            "description": "My first project",
            "s3_url": "s3://megacorp-bucket/projects/emr",
            "prediction_preference": "balanced",
        }
    )

    s3 = boto.client("s3")
    s3_stubber = Stubber(s3)

    run_timestamp = parse("2023-03-07T04:14:28Z")
    last_modified = parse("2023-03-07T05:14:28Z")
    project_id = "4f5fe783-df74-4d64-adad-a635d6319579"
    run_id = "f84639ed-7a6a-4496-81e1-b5ba8fa8b6ce"
    run_prefix = f"projects/emr/{project_id}/{run_timestamp.strftime(TIME_FORMAT)}/{run_id}"
    event_log_key = f"{run_prefix}/eventlog/application_1678162862227_0001"
    bucket = "megacorp-bucket"

    s3_stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": event_log_key, "LastModified": last_modified}]},
        {
            "Bucket": bucket,
            "Prefix": f"projects/emr/{project_id}/",
        },
    )
    s3_stubber.add_response(
        "put_object",
        {
            "ETag": '"14fe4f49fffffffffff9afbaaaaaaaa9"',
            "ResponseMetadata": {
                "HTTPHeaders": {
                    "content-length": "0",
                    "date": "Wed, 09 Apr 2022 " "20:35:42 GMT",
                    "etag": '"14fe4f49fffffffffff9afbaaaaaaaa9"',
                    "server": "AmazonS3",
                },
                "HTTPStatusCode": 200,
                "HostId": "GEHrJmjk76Ug/clCVUwimbmIjTTb2S4kU0lLg3Ylj8GKrAIsv5+S7AFb2cRkCLd+mpptmxfubLM=",
                "RequestId": "A8FFFFFFF84C3A77",
                "RetryAttempts": 0,
            },
            "VersionId": "Dbc0gbLVEN4N5F4oz7Hhek0Xd82Mdgyo",
        },
        {"Body": ANY, "Bucket": bucket, "Key": f"{run_prefix}/emr-cluster-report.json"},
    )

    emr = boto.client("emr")
    emr_stubber = Stubber(emr)

    cluster_id = "j-14QV64S2PV1Y2"

    emr_stubber.add_response(
        "list_clusters",
        {
            "Clusters": [
                {"Id": cluster_id, "Status": {"StateChangeReason": {"Code": "ALL_STEPS_COMPLETED"}}}
            ]
        },
        {
            "CreatedBefore": last_modified,
            "CreatedAfter": run_timestamp,
            "ClusterStates": ["TERMINATED"],
        },
    )

    emr_stubber.add_response(
        "describe_cluster",
        {"Cluster": {"Id": cluster_id, "Tags": [{"Key": "sync:run-id", "Value": run_id}]}},
        {"ClusterId": cluster_id},
    )

    def client_patch(name, **kwargs):
        if name == "s3":
            return s3
        elif name == "emr":
            return emr

    with s3_stubber, emr_stubber, patch("boto3.client") as mock_client:
        mock_client.side_effect = client_patch
        result = get_project_cluster_report(project_id).result

    assert not DeepDiff(cluster_report, result[0])
    assert f"s3://{bucket}/{event_log_key}" == result[1]
