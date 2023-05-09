import abc
from abc import ABC, abstractmethod

import httpx
import orjson
from tenacity import RetryError, Retrying, TryAgain, stop_after_attempt, wait_exponential_jitter

from sync import __version__

USER_AGENT = f"Sync Library/{__version__} (syncsparkpy)"

DEFAULT_RETRYABLE_STATUS_CODES: set[httpx.codes] = {
    httpx.codes.REQUEST_TIMEOUT,
    httpx.codes.TOO_EARLY,
    httpx.codes.TOO_MANY_REQUESTS,
    httpx.codes.INTERNAL_SERVER_ERROR,
    httpx.codes.BAD_GATEWAY,
    httpx.codes.SERVICE_UNAVAILABLE,
    httpx.codes.GATEWAY_TIMEOUT,
}


def encode_json(obj: dict) -> tuple[dict, str]:
    # "%Y-%m-%dT%H:%M:%SZ"
    options = orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_NAIVE_UTC

    json = orjson.dumps(obj, option=options).decode()

    return {
        "Content-Length": str(len(json)),
        "Content-Type": "application/json",
    }, json


class RetryableHTTPClient:
    """
    Smaller wrapper around httpx.Client/AsyncClient to contain retrying logic that httpx does not offer natively
    """

    def __init__(self, client: httpx.Client | httpx.AsyncClient):
        self._client: httpx.Client | httpx.AsyncClient = client

    def _send_request(self, request: httpx.Request) -> httpx.Response:
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential_jitter(initial=2, max=10, jitter=2),
            ):
                with attempt:
                    response = self._client.send(request)
                    if response.status_code in DEFAULT_RETRYABLE_STATUS_CODES:
                        raise TryAgain
        except RetryError:
            # If we max out on retries, then just bail and log the error we got
            pass

        return response

    async def _send_request_async(self, request: httpx.Request) -> httpx.Response:
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential_jitter(initial=2, max=10, jitter=2),
            ):
                with attempt:
                    response = await self._client.send(request)
                    if response.status_code in DEFAULT_RETRYABLE_STATUS_CODES:
                        raise TryAgain
        except RetryError:
            # If we max out on retries, then just bail and log the error we got
            pass

        return response
