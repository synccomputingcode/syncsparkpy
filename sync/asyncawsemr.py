from sync.asyncapi.predictions import create_prediction, wait_for_prediction
from sync.awsemr import _get_eventlog_url_from_cluster_report, get_cluster_report
from sync.models import Platform, Response


async def get_prediction_for_cluster(
    cluster_id: str, preference: str = None, region_name: str = None
) -> Response[dict]:
    """Creates a prediction (see :py:func:`~create_prediction_for_cluster`) and returns it when it's ready.

    :param cluster_id: EMR cluster ID
    :type cluster_id: str
    :param preference: preferred solution defaults to None
    :type preference: str, optional
    :param region_name: AWS region name, defaults to None
    :type region_name: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    prediction_response = await create_prediction_for_cluster(cluster_id, region_name)
    if prediction_response.error:
        return prediction_response

    return await wait_for_prediction(prediction_response.result, preference)


async def create_prediction_for_cluster(cluster_id: str, region_name: str = None) -> Response[str]:
    """If the cluster terminated successfully with an event log available in S3 a prediction based
    on such is created and its ID returned.

    :param cluster_id: EMR cluster ID
    :type cluster_id: str
    :param region_name: AWS region name, defaults to None
    :type region_name: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    report_response = get_cluster_report(cluster_id, region_name)
    cluster_report = report_response.result
    if cluster_report:
        eventlog_response = _get_eventlog_url_from_cluster_report(cluster_report)
        if eventlog_response.error:
            return eventlog_response

        eventlog_http_url = eventlog_response.result
        if eventlog_http_url:
            return await create_prediction(Platform.AWS_EMR, cluster_report, eventlog_http_url)

        return eventlog_response

    return report_response
