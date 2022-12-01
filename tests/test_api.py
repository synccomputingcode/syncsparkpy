import json
from unittest import TestCase, mock

from requests import HTTPError
from requests.structures import CaseInsensitiveDict

from sync.api import get_history


class MockResponse:
    def __init__(self, json_data, status_code, headers=None):
        self.json_data = json_data
        self.status_code = status_code
        self.headers = CaseInsensitiveDict(headers)

    @classmethod
    def ok(cls, json_path: str):
        with open(json_path) as json_in:
            return cls(json.load(json_in), 200)

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.status_code >= 300:
            raise HTTPError()


@mock.patch("sync.auth.get_auth_header", mock.Mock(return_value={"Authorization": "Bearer nope"}))
class APITests(TestCase):
    @mock.patch(
        "requests.get",
        mock.Mock(return_value=MockResponse.ok("tests/data/predictions_response.json")),
    )
    def test_history(self):
        history = get_history()
        assert history
