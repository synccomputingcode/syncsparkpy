"""Project functions
"""

import logging
from typing import List

from sync.api.predictions import get_predictions
from sync.clients.sync import get_default_client
from sync.models import Preference, ProjectError, Response

logger = logging.getLogger()


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
    app_id: str,
    description: str = None,
    s3_url: str = None,
    prediction_preference: Preference = Preference.ECONOMY,
    prediction_params: dict = None,
) -> Response[dict]:
    """Creates a Sync project for tracking and optimizing Apache Spark applications

    :param app_id: Apache Spark application name
    :type app_id: str
    :param description: application description, defaults to None
    :type description: str, optional
    :param s3_url: S3 URL under which to store project configurations and logs, defaults to None
    :type s3_url: str, optional
    :param prediction_preference: preferred prediction solution, defaults to `Preference.ECONOMY`
    :type prediction_preference: Preference, optional
    :param prediction_params: dictionary of prediction parameters, defaults to None. Valid options are documented here - https://developers.synccomputing.com/reference/create_project_v1_projects_post
    :type prediction_preference: dict, optional
    :return: the newly created project
    :rtype: Response[dict]
    """
    return Response(
        **get_default_client().create_project(
            {
                "app_id": app_id,
                "description": description,
                "s3_url": s3_url,
                "prediction_preference": prediction_preference,
                "prediction_params": prediction_params,
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
    s3_url: str = None,
    prediction_preference: Preference = None,
    prediction_params: dict = None,
) -> Response[dict]:
    """Updates a project's mutable properties

    :param project_id: project ID
    :type project_id: str
    :param description: description, defaults to None
    :type description: str, optional
    :param s3_url: location of project event logs and configurations, defaults to None
    :type s3_url: str, optional
    :param prediction_preference: default preference for predictions, defaults to None
    :type prediction_preference: Preference, optional
    :param prediction_params: dictionary of prediction parameters, defaults to None. Valid options are documented here - https://developers.synccomputing.com/reference/update_project_v1_projects__project_id__put
    :type prediction_preference: dict, optional
    :return: updated project
    :rtype: Response[dict]
    """
    project_update = {}
    if description:
        project_update["description"] = description
    if s3_url:
        project_update["s3_url"] = s3_url
    if prediction_preference:
        project_update["prediction_preference"] = prediction_preference
    if prediction_params:
        project_update["prediction_params"] = prediction_params

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
