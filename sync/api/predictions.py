"""Prediction functions
"""

import io
import logging
from time import sleep
from urllib.parse import urlparse

import boto3 as boto
import httpx

from sync.clients.sync import get_default_client
from sync.models import Platform, PredictionError, Response

logger = logging.getLogger(__name__)


def get_products() -> Response[list[str]]:
    """Get supported platforms

    :return: list of platform names
    :rtype: Response[list[str]]
    """
    response = get_default_client().get_products()
    return Response(**response)


def generate_prediction(
    platform: Platform, cluster_report: dict, eventlog_url: str, preference: str = None
) -> Response[dict]:
    """Create and return prediction

    :param platform: e.g. "aws-emr"
    :type platform: Platform
    :param cluster_report: cluster report
    :type cluster_report: dict
    :param eventlog_url: Apache Spark event log URL
    :type eventlog_url: str
    :param preference: prediction preference, defaults to None
    :type preference: str, optional
    :return: prediction object
    :rtype: Response[dict]
    """
    response = create_prediction(platform, cluster_report, eventlog_url)

    if prediction_id := response.result:
        return wait_for_prediction(prediction_id, preference)

    return response


def wait_for_prediction(prediction_id: str, preference: str = None) -> Response[dict]:
    """Get a prediction, wait if it's not ready

    :param prediction_id: prediction ID
    :type prediction_id: str
    :param preference: prediction preference, defaults to None
    :type preference: str, optional
    :return: prediction object
    :rtype: Response[dict]
    """
    response = wait_for_final_prediction_status(prediction_id)

    if result := response.result:
        if result == "SUCCESS":
            return get_prediction(prediction_id, preference)

        return Response(error=PredictionError(message="Prediction failed"))

    return response


def get_prediction(prediction_id: str, preference: str = None) -> Response[dict]:
    """Get a prediction, don't wait

    :param prediction_id: prediction ID
    :type prediction_id: str
    :param preference: prediction preference, defaults to None
    :type preference: str, optional
    :return: prediction object
    :rtype: Response[dict]
    """
    response = get_default_client().get_prediction(
        prediction_id, {"preference": preference} if preference else None
    )

    if result := response.get("result"):
        return Response(result=result)

    return Response(**response)


def get_status(prediction_id: str) -> Response[str]:
    """Get prediction status

    :param prediction_id: prediction ID
    :type prediction_id: str
    :return: prediction status, e.g. "SUCCESS"
    :rtype: Response[str]
    """
    response = get_default_client().get_prediction_status(prediction_id)

    if result := response.get("result"):
        return Response(result=result["status"])

    return Response(**response)


def get_predictions(
    product: str = None, project_id: str = None, preference: str = None
) -> Response[list[dict]]:
    """Get predictions

    :param product: platform to filter by, e.g. "aws-emr", defaults to None
    :type product: str, optional
    :param project_id: project to filter by, defaults to None
    :type project_id: str, optional
    :param preference: prediction preference, defaults to None
    :type preference: str, optional
    :return: list of prediction objects
    :rtype: Response[list[dict]]
    """
    params = {}
    if product:
        params["products"] = [product]
    if project_id:
        params["project_id"] = project_id
    if preference:
        params["preference"] = preference

    response = get_default_client().get_predictions(params)

    if response.get("result") is not None:
        return Response(result=response["result"])

    return Response(**response)


def wait_for_final_prediction_status(prediction_id: str) -> Response[str]:
    """Wait for and return terminal prediction status

    :param prediction_id: prediction ID
    :type prediction_id: str
    :return: prediction status, e.g. "SUCCESS"
    :rtype: Response[str]
    """
    while response := get_default_client().get_prediction_status(prediction_id):
        if result := response.get("result"):
            if result["status"] in ("SUCCESS", "FAILURE"):
                return Response(result=result["status"])
        else:
            return Response(**response)

        logger.info("Waiting for prediction")
        sleep(10)

    return Response(error=PredictionError(message="Failed to get prediction status"))


def create_prediction_with_eventlog_bytes(
    platform: Platform,
    cluster_report: dict,
    eventlog_name: str,
    eventlog_bytes: bytes,
    project_id: str = None,
) -> Response[str]:
    """Creates a prediction giving event log bytes instead of a URL

    :param platform: platform, e.g. "aws-emr"
    :type platform: Platform
    :param cluster_report: cluster report
    :type cluster_report: dict
    :param eventlog_name: name of event log (extension is important)
    :type eventlog_name: str
    :param eventlog_bytes: encoded event log
    :type eventlog_bytes: bytes
    :param project_id: ID of project to which the prediction belongs, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    response = get_default_client().create_prediction(
        {
            "project_id": project_id,
            "product_code": platform,
            "configs": cluster_report,
        }
    )

    if response.get("error"):
        return Response(**response)

    upload_details = response["result"]["upload_details"]
    log_response = httpx.post(
        upload_details["url"],
        data={
            **upload_details["fields"],
            "key": upload_details["fields"]["key"].replace("${filename}", eventlog_name),
        },
        files={"file": io.BytesIO(eventlog_bytes)},
    )
    if not log_response.status_code == httpx.codes.NO_CONTENT:
        return Response(error=PredictionError(message="Failed to upload event log"))

    return Response(result=response["result"]["prediction_id"])


def create_prediction(
    platform: Platform, cluster_report: dict, eventlog_url: str, project_id: str = None
) -> Response[str]:
    """Create prediction

    :param platform: platform, e.g. "aws-emr"
    :type platform: Platform
    :param cluster_report: cluster report
    :type cluster_report: dict
    :param eventlog_url: event log URL
    :type eventlog_url: str
    :param project_id: ID of project to which the prediction belongs, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    scheme = urlparse(eventlog_url).scheme
    if scheme == "s3":
        response = generate_presigned_url(eventlog_url)
        if response.error:
            return response
        eventlog_http_url = response.result
    elif scheme in {"http", "https"}:
        eventlog_http_url = eventlog_url
    else:
        return Response(error=PredictionError(message="Unsupported event log URL scheme"))

    response = get_default_client().create_prediction(
        {
            "project_id": project_id,
            "product_code": platform,
            "eventlog_url": eventlog_http_url,
            "configs": cluster_report,
        }
    )

    if response.get("error"):
        return Response(**response)

    return Response(result=response["result"]["prediction_id"])


def generate_presigned_url(s3_url: str, expires_in_secs: int = 3600) -> Response[str]:
    """Generates presigned HTTP URL for S3 URL

    :param s3_url: URL of object in S3
    :type s3_url: str
    :param expires_in_secs: number of seconds after which presigned URL expires, defaults to 3600
    :type expires_in_secs: int, optional
    :return: presigned URL
    :rtype: Response[str]
    """
    parsed_s3_url = urlparse(s3_url)

    s3 = boto.client("s3")
    return Response(
        result=s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": parsed_s3_url.netloc, "Key": parsed_s3_url.path.lstrip("/")},
            ExpiresIn=expires_in_secs,
        )
    )
