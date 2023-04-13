import logging
from typing import Generator

import httpx

from ..config import DB_CONFIG
from . import USER_AGENT, encode_json

logger = logging.getLogger(__name__)


class DatabricksAuth(httpx.Auth):
    def __init__(self, token: str) -> None:
        self.token = token

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self.token}"

        yield request


class DatabricksClient:
    def __init__(self, base_url: str, access_token: str):
        self._client = httpx.Client(
            base_url=base_url, headers={"User-Agent": USER_AGENT}, auth=DatabricksAuth(access_token)
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

    def get_cluster_events(
        self, cluster_id: str, start_time_ms: int | None = None, end_time_ms: int | None = None
    ):
        """Fetches all ClusterEvents for a given Databricks cluster, optionally within a time window.
        Pages will be followed and returned as 1 object
        """
        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#events
        # Set limit to the maximum allowable value of 500, since we want all the cluster events anyway, and
        #  we will page if we get back any `next_page` data
        args = {"cluster_id": cluster_id, "limit": 500}
        if start_time_ms:
            args["start_time"] = start_time_ms

        if end_time_ms:
            args["end_time"] = end_time_ms

        headers, content = encode_json(args)

        response = self._send(
            self._client.build_request(
                "POST", "/api/2.0/clusters/events", headers=headers, content=content
            )
        )

        responses = [response]

        while next_args := response.get("next_page"):
            headers, content = encode_json(next_args)
            response = self._send(
                self._client.build_request(
                    "POST", "/api/2.0/clusters/events", headers=headers, content=content
                )
            )

            responses.append(response)

        all_events = {
            "events": [],
            "total_count": responses[0][
                "total_count"
            ],  # total_count will be the same for all API responses
        }
        for response in responses:
            # Databricks returns cluster events from most recent --> oldest, so our paginated responses will contain
            #  older and older events as we get through them.
            all_events["events"].extend(response["events"])

        return all_events

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
        response = self._client.send(request)

        if response.status_code >= 200 and response.status_code < 300:
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
        print(conf.host)
        _sync_client = DatabricksClient(conf.host, conf.token)
        print(_sync_client)
    return _sync_client
