import json
import sys
from typing import ClassVar, Union

import httpx
from tenacity import (
    AsyncRetrying,
    Retrying,
    TryAgain,
    stop_after_attempt,
    wait_exponential_jitter,
)

from sync import __version__
from sync.utils.json import DateTimeEncoderNaiveUTCDropMicroseconds

# inclue python version
USER_AGENT = (
    f"Sync Library/{__version__} (syncsparkpy) Python/{'.'.join(map(str, sys.version_info[:3]))}"
)
DATABRICKS_USER_AGENT = "sync-gradient"


def encode_json(obj: dict) -> tuple[dict, str]:
    # "%Y-%m-%dT%H:%M:%SZ"

    json_obj = json.dumps(obj, cls=DateTimeEncoderNaiveUTCDropMicroseconds)

    return {
        "Content-Length": str(len(json_obj)),
        "Content-Type": "application/json",
    }, json_obj


class RetryableHTTPClient:
    """
    Smaller wrapper around httpx.Client/AsyncClient to contain retrying logic that httpx does not offer natively
    """

    _DEFAULT_RETRYABLE_STATUS_CODES: ClassVar[set[httpx.codes]] = {
        httpx.codes.REQUEST_TIMEOUT,
        httpx.codes.TOO_EARLY,
        httpx.codes.TOO_MANY_REQUESTS,
        httpx.codes.INTERNAL_SERVER_ERROR,
        httpx.codes.BAD_GATEWAY,
        httpx.codes.SERVICE_UNAVAILABLE,
        httpx.codes.GATEWAY_TIMEOUT,
    }

    def __init__(self, client: Union[httpx.Client, httpx.AsyncClient]):
        self._client: Union[httpx.Client, httpx.AsyncClient] = client

    def _send_request(self, request: httpx.Request) -> httpx.Response:
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(20),
                wait=wait_exponential_jitter(initial=2, max=10, jitter=2),
                reraise=True,
            ):
                with attempt:
                    response = self._client.send(request)
                    if response.status_code in self._DEFAULT_RETRYABLE_STATUS_CODES:
                        raise TryAgain()
        except TryAgain:
            # If we max out on retries, then return the bad response back to the caller to handle as appropriate
            pass

        return response

    async def _send_request_async(self, request: httpx.Request) -> httpx.Response:
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(20),
                wait=wait_exponential_jitter(initial=2, max=10, jitter=2),
                reraise=True,
            ):
                with attempt:
                    response = await self._client.send(request)
                    if response.status_code in self._DEFAULT_RETRYABLE_STATUS_CODES:
                        raise TryAgain()
        except TryAgain:
            # If we max out on retries, then return the bad response back to the caller to handle as appropriate
            pass

        return response
