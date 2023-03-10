import logging
from asyncio import sleep
from urllib.parse import urlparse

from ..api.predictions import generate_presigned_url
from ..clients.sync import get_default_async_client
from ..models import Error, Response

logger = logging.getLogger(__name__)


async def generate_prediction(cluster_config: dict, eventlog_url: str) -> Response[dict]:
    response = await create_prediction(cluster_config, eventlog_url)

    if prediction_id := response.result:
        return await wait_for_prediction(prediction_id)

    return response


async def wait_for_prediction(prediction_id: str) -> Response[dict]:
    response = await wait_for_final_prediction_status(prediction_id)

    if result := response.result:
        if result == "SUCCESS":
            return await get_prediction(prediction_id)

        return Response(error=Error(code="Prediction Error", message="Prediction failed"))

    return response


async def get_prediction(prediction_id: str) -> Response[dict]:
    response = await get_default_async_client().get_prediction(prediction_id)

    if result := response.get("result"):
        return Response(result=result)

    logger.error(f"{response['error']['code']}: {response['error']['message']}")
    return Response(error=Error(code="Prediction Error", message="Failure getting prediction"))


async def get_predictions(product: str = None, project_id: str = None) -> Response[dict]:
    params = {}
    if product:
        params["products"] = [product]
    if project_id:
        params["project_id"] = project_id
    response = await get_default_async_client().get_predictions(params)

    if response.get("result") is not None:
        return Response(result=response["result"])

    logger.error(f"{response['error']['code']}: {response['error']['message']}")
    return Response(error=Error(code="Prediction Error", message="Failure getting predictions"))


async def wait_for_final_prediction_status(prediction_id: str) -> Response[str]:
    while response := await get_default_async_client().get_prediction_status(prediction_id):
        if result := response.get("result"):
            if result["status"] in ("SUCCESS", "FAILURE"):
                return Response(result=result["status"])
        else:
            logger.error(f"{response['error']['code']}: {response['error']['message']}")
            return Response(
                error=Error(code="Prediction Error", message="Failure getting prediction status")
            )

        logger.info("Waiting for prediction")
        await sleep(10)

    return Response(error=Error(code="Prediction Error", message="Failed to get pediction status"))


async def create_prediction(cluster_config: dict, eventlog_url: str) -> Response[str]:
    parsed_eventlog_url = urlparse(eventlog_url)
    if parsed_eventlog_url.scheme == "s3":
        response = generate_presigned_url(eventlog_url)
        if response.error:
            return response
        eventlog_http_url = response.result
    else:
        eventlog_http_url = eventlog_url

    project_id = None
    for tag in cluster_config["Cluster"]["Tags"]:
        if tag["Key"] == "sync:project-id":
            project_id = tag["Value"]
            break

    response = await get_default_async_client().create_prediction(
        {
            "project_id": project_id,
            "product_code": "aws-emr",
            "eventlog_url": eventlog_http_url,
            "configs": cluster_config,
        }
    )

    if result := response.get("result"):
        return Response(result=result["prediction_id"])

    logger.error(f"{response['error']['code']}: {response['error']['message']}")
    return Response(error=Error(code="Prediction Error", message="Falure creating prediction"))
