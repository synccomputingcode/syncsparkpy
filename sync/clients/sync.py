import logging
from typing import Generator, List

import httpx

from ..config import API_KEY, CONFIG, APIKey
from . import USER_AGENT, RetryableHTTPClient, encode_json

logger = logging.getLogger(__name__)


class SyncAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, api_url: str, api_key: APIKey):
        # self.auth_url = f"{api_url}/v1/auth/token"
        self.auth_url = "https://dev-api.synccomputing.com/v1/auth/token"
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
        elif response.headers.get("Content-Type", "").startswith("application/json"):
            error = response.json().get("error")
            if error:
                logger.error(f"{error['code']}: {error['message']}")
            else:
                logger.error(f"{response.status_code}: Failed to authenticate")
        else:
            logger.error(f"{response.status_code}: Failed to authenticate")


class SyncClient(RetryableHTTPClient):
    def __init__(self, api_url, api_key):
        super().__init__(
            client=httpx.Client(
                base_url=api_url,
                headers={"User-Agent": USER_AGENT},
                auth=SyncAuth(api_url, api_key),
                timeout=60.0,
            )
        )

    def get_products(self) -> dict:
        return self._send(self._client.build_request("GET", "/v1/autotuner/products"))

    def create_prediction(self, prediction: dict) -> dict:
        headers, content = encode_json(prediction)
        return self._send(
            self._client.build_request(
                "POST", "/v1/autotuner/predictions", headers=headers, content=content
            )
        )

    def get_prediction(self, prediction_id, params: dict = None) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/autotuner/predictions/{prediction_id}", params=params
            )
        )

    def get_predictions(self, params: dict = None) -> List[dict]:
        return self._send(
            self._client.build_request("GET", "/v1/autotuner/predictions", params=params)
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

    def create_project_submission(self, project_id: str, submission: dict) -> dict:
        headers, content = encode_json(submission)
        return self._send(
            self._client.build_request(
                "POST", f"/v1/projects/{project_id}/submissions", headers=headers, content=content
            )
        )

    def _send(self, request: httpx.Request) -> dict:
        response = self._send_request(request)

        if 200 <= response.status_code < 300:
            return response.json()

        if response.headers.get("Content-Type", "").startswith("application/json"):
            response_json = response.json()
            if "error" in response_json:
                logger.error(
                    f"{response_json['error']['code']}: {response_json['error']['message']} Detail: {response_json['error'].get('detail')}"
                )
                return response_json

        logger.error(f"Unknown error - {response.status_code}: {response.text}")
        return {"error": {"code": "Sync API Error", "message": "Transaction failure"}}


class ASyncClient(RetryableHTTPClient):
    def __init__(self, api_url, api_key):
        super().__init__(
            client=httpx.AsyncClient(
                base_url=api_url,
                headers={"User-Agent": USER_AGENT},
                auth=SyncAuth(api_url, api_key),
                timeout=60.0,
            )
        )

    async def create_prediction(self, prediction: dict) -> dict:
        headers, content = encode_json(prediction)
        return await self._send(
            self._client.build_request(
                "POST", "/v1/autotuner/predictions", headers=headers, content=content
            )
        )

    async def get_prediction(self, prediction_id, params: dict = None) -> dict:
        return await self._send(
            self._client.build_request(
                "GET", f"/v1/autotuner/predictions/{prediction_id}", params=params
            )
        )

    async def get_predictions(self, params: dict = None) -> dict:
        return await self._send(
            self._client.build_request("GET", "/v1/autotuner/predictions", params=params)
        )

    async def get_prediction_status(self, prediction_id) -> dict:
        return await self._send(
            self._client.build_request("GET", f"/v1/autotuner/predictions/{prediction_id}/status")
        )

    async def create_project(self, project: dict) -> dict:
        headers, content = encode_json(project)
        return await self._send(
            self._client.build_request("POST", "/v1/projects", headers=headers, content=content)
        )

    async def update_project(self, project_id: str, project: dict) -> dict:
        headers, content = encode_json(project)
        return await self._send(
            self._client.build_request(
                "PUT", f"/v1/projects/{project_id}", headers=headers, content=content
            )
        )

    async def get_project(self, project_id: str) -> dict:
        return await self._send(self._client.build_request("GET", f"/v1/projects/{project_id}"))

    async def get_projects(self, params: dict = None) -> dict:
        return await self._send(self._client.build_request("GET", "/v1/projects", params=params))

    async def delete_project(self, project_id: str) -> dict:
        return await self._send(self._client.build_request("DELETE", f"/v1/projects/{project_id}"))

    async def _send(self, request: httpx.Request) -> dict:
        response = await self._send_request_async(request)

        if 200 <= response.status_code < 300:
            return response.json()

        if response.headers.get("Content-Type", "").startswith("application/json"):
            if "error" in response.json():
                return response.json()

        logger.error(f"Unknown error - {response.status_code}: {response.text}")
        return {"error": {"code": "Sync API Error", "message": "Transaction failure"}}


_sync_client: SyncClient = None


def get_default_client() -> SyncClient:
    global _sync_client
    if not _sync_client:
        _sync_client = SyncClient(CONFIG.api_url, API_KEY)
    return _sync_client


_async_sync_client: ASyncClient = None


def get_default_async_client() -> ASyncClient:
    global _async_sync_client
    if not _async_sync_client:
        _async_sync_client = ASyncClient(CONFIG.api_url, API_KEY)
    return _async_sync_client
