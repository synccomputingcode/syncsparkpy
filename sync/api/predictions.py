import io
import logging
from time import sleep
from urllib.parse import urlparse

import boto3 as boto
import httpx

from ..clients.sync import get_default_client
from ..models import Error, Platform, Response

logger = logging.getLogger(__name__)


def get_products() -> Response[list[str]]:
    response = get_default_client().get_products()
    return Response(**response)


def generate_prediction(
    platform: Platform, cluster_config: dict, eventlog_url: str, preference: str = None
) -> Response[dict]:
    response = create_prediction(platform, cluster_config, eventlog_url)

    if prediction_id := response.result:
        return wait_for_prediction(prediction_id, preference)

    return response


def wait_for_prediction(prediction_id: str, preference: str = None) -> Response[dict]:
    response = wait_for_final_prediction_status(prediction_id)

    if result := response.result:
        if result == "SUCCESS":
            return get_prediction(prediction_id, preference)

        return Response(error=Error(code="Prediction Error", message="Prediction failed"))

    return response


def get_prediction(prediction_id: str, preference: str = None) -> Response[dict]:
    response = get_default_client().get_prediction(
        prediction_id, {"preference": preference} if preference else None
    )

    if result := response.get("result"):
        return Response(result=result)

    logger.error(f"{response['error']['code']}: {response['error']['message']}")
    return Response(error=Error(code="Prediction Error", message="Failure getting prediction"))


def get_status(prediction_id: str) -> Response[str]:
    response = get_default_client().get_prediction_status(prediction_id)

    if result := response.get("result"):
        return Response(result=result["status"])

    logger.error(f"{response['error']['code']}: {response['error']['message']}")
    return Response(
        error=Error(code="Prediction Error", message="Failure getting prediction status")
    )


def get_predictions(product: str = None, project_id: str = None) -> Response[dict]:
    params = {}
    if product:
        params["products"] = [product]
    if project_id:
        params["project_id"] = project_id
    response = get_default_client().get_predictions(params)

    if response.get("result") is not None:
        return Response(result=response["result"])

    logger.error(f"{response['error']['code']}: {response['error']['message']}")
    return Response(error=Error(code="Prediction Error", message="Failure getting predictions"))


def wait_for_final_prediction_status(prediction_id: str) -> Response[str]:
    while response := get_default_client().get_prediction_status(prediction_id):
        if result := response.get("result"):
            if result["status"] in ("SUCCESS", "FAILURE"):
                return Response(result=result["status"])
        else:
            logger.error(f"{response['error']['code']}: {response['error']['message']}")
            return Response(
                error=Error(code="Prediction Error", message="Failure getting prediction status")
            )

        logger.info("Waiting for prediction")
        sleep(10)

    return Response(error=Error(code="Prediction Error", message="Failed to get pediction status"))


def create_prediction_with_eventlog_bytes(
    platform: Platform,
    cluster_config: dict,
    eventlog_name: str,
    eventlog_bytes: bytes,
    project_id: str = None,
) -> Response[str]:
    response = get_default_client().create_prediction(
        {
            "project_id": project_id,
            "product_code": platform.api_name,
            "configs": cluster_config,
        }
    )

    if response.get("error"):
        logger.error(f"{response['error']['code']}: {response['error']['message']}")
        return Response(error=Error(code="Prediction Error", message="Failure creating prediction"))

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
        return Response(error=Error(code="Prediction Error", message="Failed to upload event log"))

    return Response(result=response["result"]["prediction_id"])


def create_prediction(
    platform: Platform, cluster_config: dict, eventlog_url: str, project_id: str = None
) -> Response[str]:
    eventlog_http_url = None
    parsed_eventlog_url = urlparse(eventlog_url)
    match parsed_eventlog_url.scheme:
        case "s3":
            response = generate_presigned_url(eventlog_url)
            if response.error:
                return response
            eventlog_http_url = response.result
        case "http" | "https":
            eventlog_http_url = eventlog_url
        case _:
            return Response(
                error=Error(code="Prediction Error", message="Unsupported event log URL scheme")
            )

    response = get_default_client().create_prediction(
        {
            "project_id": project_id,
            "product_code": platform.api_name,
            "eventlog_url": eventlog_http_url,
            "configs": cluster_config,
        }
    )

    if response.get("error"):
        logger.error(f"{response['error']['code']}: {response['error']['message']}")
        return Response(error=Error(code="Prediction Error", message="Failure creating prediction"))

    return Response(result=response["result"]["prediction_id"])


def generate_presigned_url(s3_url: str, expires_in_secs: int = 3600) -> Response[str]:
    parsed_s3_url = urlparse(s3_url)

    s3 = boto.client("s3")
    return Response(
        result=s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": parsed_s3_url.netloc, "Key": parsed_s3_url.path.lstrip("/")},
            ExpiresIn=600,
        )
    )
