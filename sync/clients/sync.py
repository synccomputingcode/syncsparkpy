import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Optional, Tuple

import httpx
from platformdirs import user_cache_dir

from ..config import API_KEY, CONFIG, APIKey
from . import USER_AGENT, RetryableHTTPClient, encode_json

logger = logging.getLogger(__name__)


class SyncAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, api_url: str, api_key: APIKey):
        self.auth_url = f"{api_url}/v1/auth/token"
        self.api_key = api_key
        self._cache_file = Path(user_cache_dir("syncsparkpy")) / "auth.json"
        cache = self._get_cached_token()
        if cache:
            self._access_token, self._access_token_expires_at_utc = cache
        else:
            self._access_token = None
            self._access_token_expires_at_utc = None

    def _get_cached_token(self) -> Optional[Tuple[str, datetime]]:
        # Cache is optional, we can fail to read it and not worry
        token_file = self._cache_file / "access_token"
        if token_file.exists():
            try:
                cached_token = json.loads(token_file.read_text())
                cached_expiry = datetime.fromisoformat(cached_token["expires_at_utc"])
                cached_access_token = cached_token["access_token"]
                return (cached_access_token, cached_expiry)
            except Exception as e:
                logger.warning(
                    f"Failed to read cached access token @ {token_file}", exc_info=e
                )
        return None

    def _set_cached_token(self, access_token: str, expires_at_utc: datetime):
        # Cache is optional, we can fail to read it and not worry
        token_file = self._cache_file / "access_token"
        try:
            token_file.write_text(
                json.dumps(
                    {
                        "access_token": access_token,
                        "expires_at_utc": expires_at_utc.isoformat(),
                    }
                )
            )
        except Exception as e:
            logger.warning(
                f"Failed to write cached access token @ {token_file}", exc_info=e
            )

    @property
    def _access_token_valid(self) -> bool:
        if not self._access_token:
            return False
        if self._access_token_expires_at_utc:
            return datetime.now(
                tz=timezone.utc
            ) < self._access_token_expires_at_utc - timedelta(seconds=20)
        return False

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        if not self._access_token_valid:
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
        return httpx.Request(
            "POST", self.auth_url, json=self.api_key.dict(by_alias=True)
        )

    def update_access_token(self, response: httpx.Response):
        if response.status_code == httpx.codes.OK:
            auth = response.json()
            self._access_token = auth["result"]["access_token"]
            self._access_token_expires_at_utc = auth["result"]["expires_at_utc"]
            self._set_cached_token(
                self._access_token, self._access_token_expires_at_utc
            )
        elif response.headers.get("Content-Type", "").startswith("application/json"):
            error = response.json().get("error")
            if error:
                logger.error(f"{error['code']}: {error['message']}")
            else:
                logger.error(f"{response.status_code}: Failed to authenticate")
        else:
            logger.error(f"{response.status_code}: Failed to authenticate")


class SyncClient(RetryableHTTPClient):
    def __init__(self, api_url: str, api_key: APIKey):
        super().__init__(
            client=httpx.Client(
                base_url=api_url,
                headers={"User-Agent": USER_AGENT},
                auth=SyncAuth(api_url, api_key),
                timeout=60.0,
            )
        )

    def get_products(self) -> dict:
        return self._send(self._client.build_request("GET", "/v1/projects/products"))

    def create_project(self, project: dict) -> dict:
        headers, content = encode_json(project)
        return self._send(
            self._client.build_request(
                "POST", "/v1/projects", headers=headers, content=content
            )
        )

    def reset_project(self, project_id: str) -> dict:
        return self._send(
            self._client.build_request("POST", f"/v1/projects/{project_id}/reset")
        )

    def update_project(self, project_id: str, project: dict) -> dict:
        headers, content = encode_json(project)
        return self._send(
            self._client.build_request(
                "PUT", f"/v1/projects/{project_id}", headers=headers, content=content
            )
        )

    def get_project(self, project_id: str, params: dict = None) -> dict:
        return self._send(
            self._client.build_request("GET", f"/v1/projects/{project_id}", params=params)
        )

    def get_projects(self, params: dict = None) -> dict:
        return self._send(
            self._client.build_request("GET", "/v1/projects", params=params)
        )

    def delete_project(self, project_id: str) -> dict:
        return self._send(
            self._client.build_request("DELETE", f"/v1/projects/{project_id}")
        )

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

    def get_project_recommendations(self, project_id: str, params: dict = None) -> dict:
        return self._send(
            self._client.build_request(
                "GET", f"/v1/projects/{project_id}/recommendations", params=params
            )
        )

    def get_project_recommendation(
        self, project_id: str, recommendation_id: str
    ) -> dict:
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

    def get_project_submissions(self, project_id: str, params: dict = None) -> dict:
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
            self._client.build_request(
                "GET", f"/v1/databricks/workspaces/{workspace_id}"
            )
        )

    def get_workspace_configs(self) -> dict:
        return self._send(
            self._client.build_request("GET", "/v1/databricks/workspaces")
        )

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
            self._client.build_request(
                "DELETE", f"/v1/databricks/workspaces/{workspace_id}"
            )
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
    def __init__(self, api_url: str, api_key: APIKey):
        super().__init__(
            client=httpx.AsyncClient(
                base_url=api_url,
                headers={"User-Agent": USER_AGENT},
                auth=SyncAuth(api_url, api_key),
                timeout=60.0,
            )
        )

    async def create_project(self, project: dict) -> dict:
        headers, content = encode_json(project)
        return await self._send(
            self._client.build_request(
                "POST", "/v1/projects", headers=headers, content=content
            )
        )

    async def update_project(self, project_id: str, project: dict) -> dict:
        headers, content = encode_json(project)
        return await self._send(
            self._client.build_request(
                "PUT", f"/v1/projects/{project_id}", headers=headers, content=content
            )
        )

    async def get_project(self, project_id: str) -> dict:
        return await self._send(
            self._client.build_request("GET", f"/v1/projects/{project_id}")
        )

    async def get_projects(self, params: dict = None) -> dict:
        return await self._send(
            self._client.build_request("GET", "/v1/projects", params=params)
        )

    async def delete_project(self, project_id: str) -> dict:
        return await self._send(
            self._client.build_request("DELETE", f"/v1/projects/{project_id}")
        )

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
