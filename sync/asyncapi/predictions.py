import logging
from asyncio import sleep
from urllib.parse import urlparse

from sync.api.predictions import generate_presigned_url
from sync.clients.sync import get_default_async_client
from sync.models import Platform, PredictionError, Response

logger = logging.getLogger(__name__)


async def generate_prediction(
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
    response = await create_prediction(platform, cluster_report, eventlog_url)

    if prediction_id := response.result:
        return await wait_for_prediction(prediction_id, preference)

    return response


async def wait_for_prediction(prediction_id: str, preference: str) -> Response[dict]:
    """Get a prediction, wait if it's not ready

    :param prediction_id: prediction ID
    :type prediction_id: str
    :param preference: prediction preference, defaults to None
    :type preference: str, optional
    :return: prediction object
    :rtype: Response[dict]
    """
    response = await wait_for_final_prediction_status(prediction_id)

    if result := response.result:
        if result == "SUCCESS":
            return await get_prediction(prediction_id, preference)

        return Response(error=PredictionError(message="Prediction failed"))

    return response


async def get_prediction(prediction_id: str, preference: str = None) -> Response[dict]:
    """Get a prediction, don't wait

    :param prediction_id: prediction ID
    :type prediction_id: str
    :param preference: prediction preference, defaults to None
    :type preference: str, optional
    :return: prediction object
    :rtype: Response[dict]
    """
    response = await get_default_async_client().get_prediction(
        prediction_id, {"preference": preference} if preference else None
    )

    if result := response.get("result"):
        return Response(result=result)

    return Response(**response)


async def get_predictions(
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

    response = await get_default_async_client().get_predictions(params)

    if response.get("result") is not None:
        return Response(result=response["result"])

    return Response(**response)


async def wait_for_final_prediction_status(prediction_id: str) -> Response[str]:
    """Wait for and return terminal prediction status

    :param prediction_id: prediction ID
    :type prediction_id: str
    :return: prediction status, e.g. "SUCCESS"
    :rtype: Response[str]
    """
    while response := await get_default_async_client().get_prediction_status(prediction_id):
        if result := response.get("result"):
            if result["status"] in ("SUCCESS", "FAILURE"):
                return Response(result=result["status"])
        else:
            return Response(**response)

        logger.info("Waiting for prediction")
        await sleep(10)

    return Response(error=PredictionError(message="Failed to get prediction status"))


async def create_prediction(
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

    response = await get_default_async_client().create_prediction(
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
