"""Test for the dispatch utils module."""

from contextlib import AbstractContextManager, nullcontext
from typing import Any, Final
from unittest.mock import Mock, patch

import pytest
import requests

from bfb_delivery.lib.dispatch.utils import get_responses

BASE_URL: Final[str] = "http://example.com/api/v2/stops"


@pytest.mark.parametrize(
    "responses, expected_result, error_context",
    [
        (
            [
                {
                    "json.return_value": {"data": [1, 2, 3], "nextPageToken": None},
                    "status_code": 200,
                }
            ],
            [{"data": [1, 2, 3], "nextPageToken": None}],
            nullcontext(),
        ),
        (
            [
                {
                    "json.return_value": {"data": [1], "nextPageToken": "abc"},
                    "status_code": 200,
                },
                {
                    "json.return_value": {"data": [2], "nextPageToken": None},
                    "status_code": 200,
                },
            ],
            [{"data": [1], "nextPageToken": "abc"}, {"data": [2], "nextPageToken": None}],
            nullcontext(),
        ),
        (
            [
                {"json.return_value": {}, "status_code": 429},
                {"json.return_value": {}, "status_code": 429},
                {
                    "json.return_value": {"data": [3], "nextPageToken": "asfg"},
                    "status_code": 200,
                },
                {"json.return_value": {}, "status_code": 429},
                {
                    "json.return_value": {"data": [54], "nextPageToken": None},
                    "status_code": 200,
                },
            ],
            [{"data": [3], "nextPageToken": "asfg"}, {"data": [54], "nextPageToken": None}],
            nullcontext(),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 400,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError(),
                }
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 401,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError(),
                }
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 403,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError(),
                }
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 404,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError(),
                }
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 500,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError(),
                }
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
    ],
)
def test_get_responses_returns(
    responses: list[dict[str, Any]],
    expected_result: list[dict[str, Any]],
    error_context: AbstractContextManager,
) -> None:
    """Test get_responses function."""
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [Mock(**resp) for resp in responses]

        with error_context:
            result = get_responses(BASE_URL)
            assert result == expected_result

        assert mock_get.call_count == len(responses)


@pytest.mark.parametrize(
    "params, responses",
    [
        (
            "",
            [
                {
                    "json.return_value": {"data": [1, 2, 3], "nextPageToken": None},
                    "status_code": 200,
                }
            ],
        ),
        (
            "",
            [
                {
                    "json.return_value": {"data": [1], "nextPageToken": "abc"},
                    "status_code": 200,
                },
                {
                    "json.return_value": {"data": [2], "nextPageToken": None},
                    "status_code": 200,
                },
            ],
        ),
        (
            "",
            [
                {"json.return_value": {}, "status_code": 429},
                {"json.return_value": {}, "status_code": 429},
                {
                    "json.return_value": {"data": [3], "nextPageToken": "asfg"},
                    "status_code": 200,
                },
                {"json.return_value": {}, "status_code": 429},
                {
                    "json.return_value": {"data": [54], "nextPageToken": None},
                    "status_code": 200,
                },
            ],
        ),
        (
            "?filter.startsGte=2015-12-12&filter.startsLTE=2021-06-25",
            [
                {
                    "json.return_value": {"data": [1], "nextPageToken": "abc"},
                    "status_code": 200,
                },
                {
                    "json.return_value": {"data": [2], "nextPageToken": None},
                    "status_code": 200,
                },
            ],
        ),
    ],
)
def test_get_responses_urls(responses: list[dict[str, Any]], params: str) -> None:
    """Test get_responses function."""
    base_url = f"{BASE_URL}{params}"
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [Mock(**resp) for resp in responses]

        _ = get_responses(base_url)

        expected_urls = [base_url]
        last_next_page_token = None
        for resp in responses:
            next_page_token = resp["json.return_value"].get("nextPageToken")
            if next_page_token or (not next_page_token and resp["status_code"] == 429):
                if resp["status_code"] == 429:
                    next_page_token = last_next_page_token
                last_next_page_token = next_page_token

                token_prefix = "?" if "?" not in base_url else "&"
                token = (
                    f"{token_prefix}pageToken={next_page_token}" if next_page_token else ""
                )
                expected_urls.append(f"{base_url}{token}")

        actual_urls = [call.args[0] for call in mock_get.call_args_list]

        assert actual_urls == expected_urls


# TODO: Test wait time.
