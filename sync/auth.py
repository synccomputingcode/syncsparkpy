"""
Sync API authentication utilities
"""

import requests

from . import SYNC_APIv1_URL
from .config import get_api_key


def get_auth_header() -> dict:
    api_key = get_api_key()

    response = requests.post(f"{SYNC_APIv1_URL}/auth/token", json=api_key.dict(by_alias=True))
    response.raise_for_status()

    return {"Authorization": f"Bearer {response.json()['result']['access_token']}"}
