import asyncio
import logging
import threading
from collections.abc import AsyncGenerator, Generator
from typing import Optional

import dateutil.parser
import httpx
from tenacity import (
    AsyncRetrying,
    TryAgain,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from sync.config import API_KEY, CONFIG, APIKey

from . import USER_AGENT, RetryableHTTPClient, encode_json
from .cache import (
    ACCESS_TOKEN_CACHE_CLS_TYPE,
    FileCachedToken,
    get_access_token_cache_cache,
)

logger = logging.getLogger(__name__)


class SyncAuth(httpx.Auth):
    requires_request_body = True
    requires_response_body = True

    retryable_status_codes = (
        httpx.codes.SERVICE_UNAVAILABLE,
        httpx.codes.TOO_MANY_REQUESTS,
        httpx.codes.BAD_GATEWAY,
        httpx.codes.GATEWAY_TIMEOUT,
    )

    def __init__(
        self,
        api_url: str,
        api_key: APIKey,
        access_token_cache_cls: ACCESS_TOKEN_CACHE_CLS_TYPE = FileCachedToken,
    ):
        self.auth_url = f"{api_url}/v1/auth/token"
        self.api_key = api_key

        self.cached_token = access_token_cache_cls()
        self._sync_lock = threading.RLock()
        self._async_lock = asyncio.Lock()

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential_jitter(2, 10),
        reraise=False,
        retry=retry_if_exception_type(TryAgain),
    )
    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        with self._sync_lock:
            if not self.cached_token.is_access_token_valid:
                # fetch with retry and exponential backoff
                response = yield self.build_auth_request()
                if response.status_code == httpx.codes.OK:
                    self.update_access_token(response)
                elif response.status_code in self.retryable_status_codes:
                    raise TryAgain()
                else:
                    logger.error(f"{response.status_code}: Failed to authenticate")
            if not self.cached_token.is_access_token_valid:
                raise TryAgain()
        request.headers["Authorization"] = f"Bearer {self.cached_token.access_token}"
        yield request

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        async with self._async_lock:
            while not self.cached_token.is_access_token_valid:
                try:
                    async for attempt in AsyncRetrying(
                        stop=stop_after_attempt(10),
                        wait=wait_exponential_jitter(2, 10),
                        reraise=True,
                    ):
                        with attempt:
                            # fetch with retry and exponential backoff
                            response = yield self.build_auth_request()
                            if response.status_code == httpx.codes.OK:
                                self.update_access_token(response)
                                break
                            elif response.status_code in self.retryable_status_codes:
                                raise TryAgain()
                            else:
                                logger.error(f"{response.status_code}: Failed to authenticate")
                except TryAgain:
                    # Hit maximum retries
                    logger.error("Failed to authenticate, max retries reached")
                    break
        request.headers["Authorization"] = f"Bearer {self.cached_token.access_token}"
        yield request

    def build_auth_request(self) -> httpx.Request:
        return httpx.Request("POST", self.auth_url, json=self.api_key.dict(by_alias=True))

    def update_access_token(self, response: httpx.Response):
        auth = response.json()
        access_token = auth["result"]["access_token"]
        expires_at_utc = dateutil.parser.isoparse(auth["result"]["expires_at_utc"])
        self.cached_token.set_cached_token(access_token, expires_at_utc)


class SyncClient(RetryableHTTPClient):
    def __init__(
        self,
        api_url: str,
        api_key: APIKey,
        access_token_cache_cls: ACCESS_TOKEN_CACHE_CLS_TYPE = FileCachedToken,
    ):
        super().__init__(
            client=httpx.Client(
                base_url=api_url,
                headers={"User-Agent": USER_AGENT},
                auth=SyncAuth(api_url, api_key, access_token_cache_cls=access_token_cache_cls),
                timeout=60.0,
            )
        )

    def get_products(self) -> dict:
        return self._send(self._client.build_request("GET", "/v1/projects/products"))

    def create_project(self, project: dict) -> dict:
        headers, content = encode_json(project)
        return self._send(
            self._client.build_request("POST", "/v1/projects", headers=headers, content=content)
        )

    def reset_project(self, project_id: str) -> dict:
        return self._send(self._client.build_request("POST", f"/v1/projects/{project_id}/reset"))

    def update_project(self, project_id: str, project: dict) -> dict:
        headers, content = encode_json(project)
        return self._send(
            self._client.build_request(
                "PUT", f"/v1/projects/{project_id}", headers=headers, content=content
            )
        )

    def get_project(self, project_id: str, params: Optional[dict] = None) -> dict:
        return self._send(
            self._client.build_request("GET", f"/v1/projects/{project_id}", params=params)
        )

    def get_project_cluster_template(self, project_id: str, params: Optional[dict] = None) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/projects/{project_id}/cluster/template", params=params
            )
        )

    def get_projects(self, params: Optional[dict] = None) -> dict:
        return self._send(self._client.build_request("GET", "/v1/projects", params=params))

    def delete_project(self, project_id: str) -> dict:
        return self._send(self._client.build_request("DELETE", f"/v1/projects/{project_id}"))

    def create_project_submission(self, project_id: str, submission: dict) -> dict:
        headers, content = encode_json(submission)
        return self._send(
            self._client.build_request(
                "POST",
                f"/v1/projects/{project_id}/submissions",
                headers=headers,
                content=content,
            )
        )

    def create_project_recommendation(self, project_id: str, **options) -> dict:
        headers, content = encode_json(options)
        return self._send(
            self._client.build_request(
                "POST",
                f"/v1/projects/{project_id}/recommendations",
                headers=headers,
                content=content,
            )
        )

    def get_project_recommendations(self, project_id: str, params: Optional[dict] = None) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/projects/{project_id}/recommendations", params=params
            )
        )

    def get_project_recommendation(self, project_id: str, recommendation_id: str) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/projects/{project_id}/recommendations/{recommendation_id}"
            )
        )

    def get_latest_project_recommendation(self, project_id: str) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/projects/{project_id}/recommendations?page=0&per_page=1"
            )
        )

    def get_project_submissions(self, project_id: str, params: Optional[dict] = None) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/projects/{project_id}/paged/submissions", params=params
            )
        )

    def get_project_submission(self, project_id: str, submission_id: str) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/projects/{project_id}/submissions/{submission_id}"
            )
        )

    def create_workspace_config(self, workspace_id: str, **config) -> dict:
        headers, content = encode_json(config)
        return self._send(
            self._client.build_request(
                "POST",
                f"/v1/databricks/workspaces/{workspace_id}",
                headers=headers,
                content=content,
            )
        )

    def get_workspace_config(self, workspace_id: str) -> dict:
        return self._send(
            self._client.build_request("GET", f"/v1/databricks/workspaces/{workspace_id}")
        )

    def get_workspace_configs(self) -> dict:
        return self._send(self._client.build_request("GET", "/v1/databricks/workspaces"))

    def update_workspace_config(self, workspace_id: str, **updates) -> dict:
        headers, content = encode_json(updates)
        return self._send(
            self._client.build_request(
                "PUT",
                f"/v1/databricks/workspaces/{workspace_id}",
                headers=headers,
                content=content,
            )
        )

    def delete_workspace_config(self, workspace_id: str) -> dict:
        return self._send(
            self._client.build_request("DELETE", f"/v1/databricks/workspaces/{workspace_id}")
        )

    def reset_webhook_creds(self, workspace_id: str) -> dict:
        return self._send(
            self._client.build_request(
                "POST", f"/v1/databricks/workspaces/{workspace_id}/webhook-credentials"
            )
        )

    def apply_workspace_config(self, workspace_id: str) -> dict:
        return self._send(
            self._client.build_request(
                "POST",
                f"/v1/databricks/workspaces/{workspace_id}/setup",
            )
        )

    def onboard_workflow(self, workspace_id, job_id: str, project_id: str) -> dict:
        headers, content = encode_json({"job_id": job_id, "project_id": project_id})
        return self._send(
            self._client.build_request(
                "POST",
                f"/v1/databricks/workspaces/{workspace_id}/onboard-workflow",
                headers=headers,
                content=content,
            )
        )

    def _send(self, request: httpx.Request) -> dict:
        response = self._send_request(request)

        # A temporary crutch to make this client interface consistent
        # This was added for DELETE /v1/databricks/workspaces/{workspace_id}
        if response.status_code == 204:
            return {"result": "OK"}

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
    def __init__(
        self,
        api_url: str,
        api_key: APIKey,
        access_token_cache_cls: ACCESS_TOKEN_CACHE_CLS_TYPE = FileCachedToken,
    ):
        super().__init__(
            client=httpx.AsyncClient(
                base_url=api_url,
                headers={"User-Agent": USER_AGENT},
                auth=SyncAuth(api_url, api_key, access_token_cache_cls=access_token_cache_cls),
                timeout=60.0,
            )
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

    async def get_projects(self, params: Optional[dict] = None) -> dict:
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


_sync_client: Optional[SyncClient] = None


def get_default_client() -> SyncClient:
    global _sync_client
    if not _sync_client:
        _sync_client = SyncClient(
            CONFIG.api_url,
            API_KEY,
            access_token_cache_cls=get_access_token_cache_cache(),
        )
    return _sync_client


_async_sync_client: Optional[ASyncClient] = None


def get_default_async_client() -> ASyncClient:
    global _async_sync_client
    if not _async_sync_client:
        _async_sync_client = ASyncClient(
            CONFIG.api_url,
            API_KEY,
            access_token_cache_cls=get_access_token_cache_cache(),
        )
    return _async_sync_client
