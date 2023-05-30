import httpx
import orjson
from tenacity import Retrying, TryAgain, stop_after_attempt, wait_exponential_jitter
from typing import Tuple, Union, Set

from sync import __version__

USER_AGENT = f"Sync Library/{__version__} (syncsparkpy)"


def encode_json(obj: dict) -> Tuple[dict, str]:
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

    _DEFAULT_RETRYABLE_STATUS_CODES: Set[httpx.codes] = {
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
                stop=stop_after_attempt(3),
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
            for attempt in Retrying(
                stop=stop_after_attempt(3),
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
