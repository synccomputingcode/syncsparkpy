"""
Sync API utilities
"""

import requests

from . import SYNC_APIv1_URL
from .auth import get_auth_header
from .models import Preference, Response


def create_project(s3_base_url: str, preference: Preference = Preference.BALANCED) -> Response[str]:
    """Creates a project that expects event logs and configurations at the specified location.

    .. todo::
        this should probably take a model representing the project

    :param s3_base_url: location under which logs and configuration from successive runs will be stored
    :type s3_base_url: str
    :param preference: default prediction to be applied, defaults to Preference.BALANCED
    :type preference: Preference, optional
    :return: project ID
    :rtype: Response[str]
    """


def get_history() -> Response[dict]:
    """Get predictions

    :return: predictions
    :rtype: Response[dict]
    """
    return _get("autotuner/predictions")


def get_prediction(id: str, preference: Preference) -> Response[dict]:
    """Returns a prediction for the provided preference

    :param id: prediction ID
    :type id: str
    :param preference: prediction preference
    :type preference: Preference
    :return: prediction
    :rtype: Response[dict]
    """
    return _get(f"autotuner/predictions/{id}?preference={preference}")


def _get(path) -> dict:
    response = requests.get(f"{SYNC_APIv1_URL}/{path}", headers=get_auth_header())
    response.raise_for_status()

    return response.json()["result"]
