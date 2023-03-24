from unittest.mock import Mock, patch

import boto3 as boto
import orjson
from botocore.stub import Stubber
from deepdiff import DeepDiff

from sync.awsemr import create_prediction_for_cluster, get_cluster_record
from sync.models import Response


@patch("sync.awsemr.get_cluster_record")
@patch("sync.awsemr.create_prediction")
def test_create_prediction(create_prediction, get_cluster_record):
    with open("tests/data/emr-config.json") as emr_config_fobj:
        get_cluster_record.return_value = Response(result=orjson.loads(emr_config_fobj.read()))

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
            get_cluster_record.return_value.result["Cluster"]["Id"]
        )

    assert prediction_id == response.result


def test_get_cluster_record():
    with open("tests/data/emr-config.json") as emr_config_fobj:
        emr_config = orjson.loads(emr_config_fobj.read())

    cluster_id = emr_config["Cluster"]["Id"]
    region = emr_config["Region"]

    emr = boto.client("emr")
    stubber = Stubber(emr)

    describe_response = {"Cluster": emr_config["Cluster"].copy()}
    del describe_response["Cluster"]["BootstrapActions"]
    del describe_response["Cluster"]["InstanceFleets"]

    stubber.add_response("describe_cluster", describe_response, {"ClusterId": cluster_id})
    stubber.add_response(
        "list_bootstrap_actions",
        {"BootstrapActions": emr_config["Cluster"]["BootstrapActions"]},
        {"ClusterId": cluster_id},
    )
    stubber.add_response(
        "list_instance_fleets",
        {"InstanceFleets": emr_config["Cluster"]["InstanceFleets"]},
        {"ClusterId": cluster_id},
    )
    stubber.add_response(
        "list_instances", {"Instances": emr_config["Instances"]}, {"ClusterId": cluster_id}
    )
    stubber.add_response("list_steps", {"Steps": emr_config["Steps"]}, {"ClusterId": cluster_id})

    with stubber, patch("boto3.client") as mock_client:
        mock_client.return_value = emr
        result = get_cluster_record(cluster_id, region).result

    assert not DeepDiff(emr_config, result)
