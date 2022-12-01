"""
Sync API utilities
"""

import requests

from . import SYNC_APIv1_URL
from .auth import get_auth_header
from .models import Preference


def get_history():
    return _get("autotuner/predictions")


def get_prediction(id: str, preference: Preference):
    return _get(f"autotuner/predictions/{id}?preference={preference}")


def _get(path) -> dict:
    response = requests.get(f"{SYNC_APIv1_URL}/{path}", headers=get_auth_header())
    response.raise_for_status()

    return response.json()["result"]
