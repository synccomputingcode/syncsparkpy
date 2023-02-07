import logging
from typing import Generator

import httpx
import orjson

from . import __version__
from .config import API_KEY, CONFIG, APIKey

logger = logging.getLogger(__name__)


class SyncAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, api_url: str, api_key: APIKey):
        self.auth_url = f"{api_url}/v1/auth/token"
        self.api_key = api_key
        self._access_token = None

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        if not self._access_token:
            response = yield self.build_auth_request()
            self.update_access_token(response)

        request.headers["Authorization"] = f"Bearer {self._access_token}"
        response = yield request

        if response.status_code == httpx.codes.UNAUTHORIZED:
            response = yield self.build_auth_request()
            self.update_access_token(response)

            request.headers["Authorization"] = f"Bearer {self._access_token}"
            response = yield request

    def build_auth_request(self) -> httpx.Request:
        return httpx.Request("POST", self.auth_url, json=self.api_key.dict(by_alias=True))

    def update_access_token(self, response: httpx.Response):
        if response.status_code == httpx.codes.OK:
            auth = response.json()
            self._access_token = auth["result"]["access_token"]
        else:
            if error := response.json().get("error"):
                logger.error(f"{error['code']}: {error['message']}")
            else:
                logger.error(f"{response.status_code}: Failed to authenticate")


class Sync:
    def __init__(self, api_url, api_key):
        self._client = httpx.Client(
            base_url=api_url,
            headers={"User-Agent": f"Sync SDK v{__version__}"},
            auth=SyncAuth(api_url, api_key),
        )

    def create_prediction(self, prediction: dict) -> dict:
        headers, content = encode_json(prediction)
        return self._send(
            self._client.build_request(
                "POST", "/v1/autotuner/predictions", headers=headers, content=content
            )
        )

    def get_prediction(self, prediction_id) -> dict:
        return self._send(
            self._client.build_request("GET", f"/v1/autotuner/predictions/{prediction_id}")
        )

    def get_predictions(self, params: dict = None) -> dict:
        return self._send(
            self._client.build_request("GET", "/v1/autotuner/predictions"), params=params
        )

    def get_prediction_status(self, prediction_id) -> dict:
        return self._send(
            self._client.build_request("GET", f"/v1/autotuner/predictions/{prediction_id}/status")
        )

    def create_project(self, project: dict) -> dict:
        headers, content = encode_json(project)
        return self._send(
            self._client.build_request("POST", "/v1/projects", headers=headers, content=content)
        )

    def update_project(self, project_id: str, project: dict) -> dict:
        headers, content = encode_json(project)
        return self._send(
            self._client.build_request(
                "PUT", f"/v1/projects/{project_id}", headers=headers, content=content
            )
        )

    def get_project(self, project_id: str) -> dict:
        return self._send(self._client.build_request("GET", f"/v1/projects/{project_id}"))

    def get_projects(self, params: dict = None) -> dict:
        return self._send(self._client.build_request("GET", "/v1/projects", params=params))

    def delete_project(self, project_id: str) -> dict:
        return self._send(self._client.build_request("DELETE", f"/v1/projects/{project_id}"))

    def _send(self, request: httpx.Request) -> dict:
        response = self._client.send(request)

        if response.status_code == httpx.codes.OK:
            return response.json()

        if error := response.json().get("error"):
            logger.error(f"{error['code']}: {error['message']}")
            return response.json()

        logger.error(f"{response.status_code}: Transaction failed")
        return {"error": {"code": "Unknown error", "message": "Transaction failed."}}


def encode_json(obj: dict) -> tuple[dict, str]:
    # "%Y-%m-%dT%H:%M:%SZ"
    options = orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_NAIVE_UTC

    # Drop null values - some endpoints will fail when validating null values
    nonone_obj = dict((key, value) for key, value in obj.items() if value)

    json = orjson.dumps(nonone_obj, option=options).decode()

    return {
        "Content-Length": str(len(json)),
        "Content-Type": "application/json",
    }, json


_sync_client: Sync
_sync_client = None


def get_default_client() -> Sync:
    global _sync_client
    if not _sync_client:
        _sync_client = Sync(CONFIG.api_url, API_KEY)
    return _sync_client
