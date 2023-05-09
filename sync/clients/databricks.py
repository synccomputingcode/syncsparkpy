import logging
from typing import Generator

import httpx

from ..config import DB_CONFIG
from . import USER_AGENT, RetryableHTTPClient, encode_json

logger = logging.getLogger(__name__)


class DatabricksAuth(httpx.Auth):
    def __init__(self, token: str) -> None:
        self.token = token

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self.token}"

        yield request


class DatabricksClient(RetryableHTTPClient):
    def __init__(self, base_url: str, access_token: str):
        super().__init__(
            client=httpx.Client(
                base_url=base_url,
                headers={"User-Agent": USER_AGENT},
                auth=DatabricksAuth(access_token),
            )
        )

    def create_cluster(self, config: dict) -> dict:
        headers, content = encode_json(config)
        return self._send(
            self._client.build_request(
                "POST", "/api/2.0/clusters/create", headers=headers, content=content
            )
        )

    def get_cluster(self, cluster_id: str) -> dict:
        return self._send(
            self._client.build_request(
                "GET", "/api/2.0/clusters/get", params={"cluster_id": cluster_id}
            )
        )

    def delete_cluster(self, cluster_id: str) -> dict:
        headers, content = encode_json({"cluster_id": cluster_id})
        return self._send(
            self._client.build_request(
                "POST", "/api/2.0/clusters/delete", headers=headers, content=content
            )
        )

    def get_cluster_events(self, cluster_id: str, **kwargs) -> dict:
        """Returns a single page of cluster events for the given cluster_id. **kwargs will be passed
        as-is as query parameters to the Databricks API. Refer to these docs for all possible arguments -
        https://docs.databricks.com/dev-tools/api/latest/clusters.html#events
        """
        kwargs["cluster_id"] = cluster_id

        headers, content = encode_json(kwargs)

        return self._send(
            self._client.build_request(
                "POST", "/api/2.0/clusters/events", headers=headers, content=content
            )
        )

    def get_job(self, job_id: str) -> dict:
        return self._send(
            self._client.build_request("GET", "/api/2.1/jobs/get", params={"job_id": job_id})
        )

    def create_run(self, run: dict) -> dict:
        # https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
        headers, content = encode_json(run)
        return self._send(
            self._client.build_request(
                "POST", "/api/2.1/jobs/runs/submit", headers=headers, content=content
            )
        )

    def create_job_run(self, run: dict) -> dict:
        # https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
        headers, content = encode_json(run)
        return self._send(
            self._client.build_request(
                "POST", "/api/2.1/jobs/run-now", headers=headers, content=content
            )
        )

    def get_run(self, run_id: str) -> dict:
        return self._send(
            self._client.build_request("GET", "/api/2.1/jobs/runs/get", params={"run_id": run_id})
        )

    def _send(self, request: httpx.Request) -> dict:
        response = self._send_request(request)

        if 200 <= response.status_code < 300:
            return response.json()

        if response.headers.get("Content-Type", "").startswith("application/json"):
            response_json = response.json()
            if "error_code" in response_json:
                # Though not in the documentation, the cluster API can return and "error_code" too
                # return {"error": {"code": "Databricks API Error", "message": f"{response_json['error_code']}: {response_json.get('message')}"}}
                return response_json

        # return {"error": {"code": "Databricks API Error", "message": "Transaction failure"}}
        logger.error(f"Unknown error - {response.status_code}: {response.text}")
        return {"error_code": "UNKNOWN_ERROR", "message": "Transaction failure"}


_sync_client: DatabricksClient | None = None


def get_default_client() -> DatabricksClient:
    global _sync_client
    if not _sync_client:
        conf = DB_CONFIG
        _sync_client = DatabricksClient(conf.host, conf.token)
    return _sync_client
