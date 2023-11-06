"""Project functions
"""
import io
import logging
from time import sleep
from typing import List
from urllib.parse import urlparse

import httpx

from sync.api.predictions import generate_presigned_url, get_predictions
from sync.clients.sync import get_default_client
from sync.models import (
    Platform,
    Preference,
    ProjectError,
    RecommendationError,
    Response,
    SubmissionError,
)

logger = logging.getLogger(__name__)


def get_prediction(project_id: str, preference: Preference = None) -> Response[dict]:
    """Get the latest prediction of a project

    :param project_id: project ID
    :type project_id: str
    :param preference: preferred prediction solution, defaults to project setting
    :type preference: Preference, optional
    :return: prediction object
    :rtype: Response[dict]
    """
    project_response = get_project(project_id)
    project = project_response.result
    if project:
        predictions_response = get_predictions(
            project_id=project_id, preference=preference or project.get("preference")
        )
        if predictions_response.error:
            return predictions_response

        predictions = predictions_response.result
        if predictions:
            return Response(result=predictions[0])
        return Response(error=ProjectError(message="No predictions in the project"))
    return project_response


def create_project(
    name: str,
    product_code: str,
    description: str = None,
    job_id: str = None,
    cluster_log_url: str = None,
    prediction_preference: Preference = Preference.ECONOMY,
    auto_apply_recs: bool = False,
    prediction_params: dict = None,
    app_id: str = None,
) -> Response[dict]:
    """Creates a Sync project for tracking and optimizing Apache Spark applications

    :param name: Project name
    :type name: str
    :param product_code: Product code
    :type product_code: str
    :param description: application description, defaults to None
    :type description: str, optional
    :param job_id: Databricks job ID, defaults to None
    :type job_id: str, optional
    :param cluster_log_url: S3 or DBFS URL under which to store project configurations and logs, defaults to None
    :type cluster_log_url: str, optional
    :param prediction_preference: preferred prediction solution, defaults to `Preference.ECONOMY`
    :type prediction_preference: Preference, optional
    :param auto_apply_recs: automatically apply project recommendations, defaults to False
    :type auto_apply_recs: bool, optional
    :param prediction_params: dictionary of prediction parameters, defaults to None. Valid options are documented `here <https://developers.synccomputing.com/reference/create_project_v1_projects_post>`__
    :type prediction_params: dict, optional
    :param app_id: Apache Spark application identifier, defaults to None
    :type app_id: str, optional
    :return: the newly created project
    :rtype: Response[dict]
    """
    return Response(
        **get_default_client().create_project(
            {
                "name": name,
                "product_code": product_code,
                "description": description,
                "job_id": job_id,
                "cluster_log_url": cluster_log_url,
                "prediction_preference": prediction_preference,
                "auto_apply_recs": auto_apply_recs,
                "prediction_params": prediction_params,
                "app_id": app_id,
            }
        )
    )


def get_project(project_id: str) -> Response[dict]:
    """Retrieves a project

    :param project_id: project ID
    :type project_id: str
    :return: project object
    :rtype: Response[dict]
    """
    return Response(**get_default_client().get_project(project_id))


def update_project(
    project_id: str,
    description: str = None,
    cluster_log_url: str = None,
    app_id: str = None,
    prediction_preference: Preference = None,
    auto_apply_recs: bool = None,
    prediction_params: dict = None,
    job_id: str = None,
) -> Response[dict]:
    """Updates a project's mutable properties

    :param project_id: project ID
    :type project_id: str
    :param description: description, defaults to None
    :type description: str, optional
    :param cluster_log_url: location of project event logs and configurations, defaults to None
    :type cluster_log_url: str, optional
    :param app_id: external identifier, defaults to None
    :type app_id: str, optional
    :param prediction_preference: default preference for predictions, defaults to None
    :type prediction_preference: Preference, optional
    :param auto_apply_recs: automatically apply project recommendations, defaults to None
    :type auto_apply_recs: bool, optional
    :param prediction_params: dictionary of prediction parameters, defaults to None. Valid options are documented `here <https://developers.synccomputing.com/reference/update_project_v1_projects__project_id__put>`__
    :type prediction_preference: dict, optional
    :param job_id: Databricks job id, defaults to None
    :type job_id: str, optional
    :return: updated project
    :rtype: Response[dict]
    """
    project_update = {}
    if description:
        project_update["description"] = description
    if cluster_log_url:
        project_update["cluster_log_url"] = cluster_log_url
    if app_id:
        project_update["app_id"] = app_id
    if prediction_preference:
        project_update["prediction_preference"] = prediction_preference
    if auto_apply_recs is not None:
        project_update["auto_apply_recs"] = auto_apply_recs
    if prediction_params:
        project_update["prediction_params"] = prediction_params
    if job_id:
        project_update["job_id"] = job_id

    return Response(
        **get_default_client().update_project(
            project_id,
            project_update,
        )
    )


def get_project_by_app_id(app_id: str) -> Response[dict]:
    """Retrieves a project by app ID

    :param app_id: app ID
    :type app_id: str
    :return: project or error if none exists for the app ID
    :rtype: Response[dict]
    """
    response = get_default_client().get_projects({"app_id": app_id})
    if response.get("error"):
        return Response(**response)

    projects = response.get("result")
    if projects:
        return Response(result=projects[0])

    return Response(error=ProjectError(message=f"No project found for '{app_id}'"))


def get_projects(app_id: str = None) -> Response[List[dict]]:
    """Returns all projects authorized by the API key

    :param app_id: app ID to filter by, defaults to None
    :type app_id: str, optional
    :return: projects
    :rtype: Response[list[dict]]
    """
    return Response(**get_default_client().get_projects(params={"app_id": app_id}))


def delete_project(project_id: str) -> Response[str]:
    """Deletes a project

    :param project_id: project ID
    :type project_id: str
    :return: confirmation message
    :rtype: Response[str]
    """
    return Response(**get_default_client().delete_project(project_id))


def create_project_submission(
    platform: Platform, cluster_report: dict, eventlog_url: str, project_id: str
) -> Response[str]:
    """Create a submission

    :param platform: platform, e.g. "aws-emr"
    :type platform: Platform
    :param cluster_report: cluster report
    :type cluster_report: dict
    :param eventlog_url: event log URL
    :type eventlog_url: str
    :param project_id: ID of project to which the submission belongs
    :type project_id: str
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
        return Response(error=SubmissionError(message="Unsupported event log URL scheme"))

    payload = {
        "product": platform,
        "cluster_report": cluster_report,
        "event_log_uri": eventlog_http_url,
    }

    logger.info(payload)

    response = get_default_client().create_project_submission(
        project_id,
        payload,
    )

    if response.get("error"):
        return Response(**response)

    return Response(result=response["result"]["submission_id"])


def _clear_cluster_report_errors(cluster_report_orig: dict) -> dict:
    """Clears error messages from the cluster_events field
    This circumvents issues where certain strange characters in the error fields of Azure cluster
    reports were causing the client to throw errors when trying to make submissions.

    :param cluster_report_orig: cluster_report
    :type cluster_report_orig: dict
    :return: cleared cluster report
    :rtype: dict
    """
    cluster_report = cluster_report_orig.copy()

    def clear_error(event: dict):
        try:
            del event["details"]["reason"]["parameters"]["azure_error_message"]
        except KeyError:
            pass
        try:
            del event["details"]["reason"]["parameters"]["databricks_error_message"]
        except KeyError:
            pass

    try:
        list(map(clear_error, cluster_report["cluster_events"]["events"]))
    except KeyError:
        pass
    return cluster_report


def create_project_submission_with_eventlog_bytes(
    platform: Platform,
    cluster_report: dict,
    eventlog_name: str,
    eventlog_bytes: bytes,
    project_id: str,
) -> Response[str]:
    """Creates a submission given event log bytes instead of a URL

    :param platform: platform, e.g. "aws-emr"
    :type platform: Platform
    :param cluster_report: cluster report
    :type cluster_report: dict
    :param eventlog_name: name of event log (extension is important)
    :type eventlog_name: str
    :param eventlog_bytes: encoded event log
    :type eventlog_bytes: bytes
    :param project_id: ID of project to which the submission belongs
    :type project_id: str
    :return: prediction ID
    :rtype: Response[str]
    """
    # TODO - best way to handle "no eventlog"
    cluster_report_clear = _clear_cluster_report_errors(cluster_report)
    response = get_default_client().create_project_submission(
        project_id, {"product_code": platform, "cluster_report": cluster_report_clear}
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
        return Response(error=SubmissionError(message="Failed to upload event log"))

    return Response(result=response["result"]["submission_id"])


def create_project_recommendation(project_id: str, **options) -> Response[str]:
    """Creates a prediction given a project id

    :param project_id: ID of project to which the prediction belongs, defaults to None
    :type project_id: str, optional
    :return: prediction ID
    :rtype: Response[str]
    """
    response = get_default_client().create_project_recommendation(project_id, **options)

    if response.get("error"):
        return Response(**response)

    return Response(result=response["result"]["id"])


def wait_for_recommendation(project_id: str, recommendation_id: str) -> Response[dict]:
    """Get a recommendation, wait if it's not ready

    :param project_id: project ID
    :type project_id: str
    :param recommendation_id: recommendation ID
    :type recommendation_id: str
    :return: recommendation object
    :rtype: Response[dict]
    """
    response = get_project_recommendation(project_id, recommendation_id)
    while response:
        result = response.result
        if result:
            if result["state"] == "SUCCESS":
                return Response(result=result)
            if result["state"] == "FAILURE":
                return Response(error=RecommendationError(message="Recommendation failed"))
        logger.info("Waiting for recommendation")
        sleep(10)
        response = get_project_recommendation(project_id, recommendation_id)


def get_project_recommendation(project_id: str, recommendation_id: str) -> Response[dict]:
    """Get a specific recommendation for a project id

    :param project_id: project ID
    :type project_id: str
    :param recommendation_id: recommendation ID
    :type recommendation_id: str
    :return: recommendation object
    :rtype: Response[dict]
    """
    response = get_default_client().get_project_recommendation(project_id, recommendation_id)

    if response.get("error"):
        return Response(**response)

    return Response(result=response["result"])
