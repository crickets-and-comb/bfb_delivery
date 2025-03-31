"""Test for the dispatch utils module."""

from contextlib import AbstractContextManager, nullcontext
from typing import Any, Final
from unittest.mock import Mock, patch

import pytest
import requests
from typeguard import typechecked

from comb_utils import get_responses

from bfb_delivery.lib.dispatch.api_callers import PagedResponseGetterBFB
from bfb_delivery.lib.dispatch.utils import get_circuit_key

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
@typechecked
def test_get_responses_returns(
    responses: list[dict[str, Any]],
    expected_result: list[dict[str, Any]] | None,
    error_context: AbstractContextManager,
) -> None:
    """Test get_responses function."""
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [Mock(**resp) for resp in responses]

        with error_context:
            result = get_responses(url=BASE_URL, paged_response_class=PagedResponseGetterBFB)
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
@typechecked
def test_get_responses_urls(responses: list[dict[str, Any]], params: str) -> None:
    """Test get_responses function."""
    base_url = f"{BASE_URL}{params}"
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [Mock(**resp) for resp in responses]

        _ = get_responses(url=base_url, paged_response_class=PagedResponseGetterBFB)

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

        actual_urls = [call[1]["url"] for call in mock_get.call_args_list]

        assert actual_urls == expected_urls


# No longer needed once api_callers under test.
# Difficult to test here since wait is a class var and depends on what ran before.
# Leaving for reference for now.
# @typechecked
# def test_get_responses_wait_time() -> None:
#     """Test get_responses doubles wait times passed to requests.get after 429 response."""
#     responses = [
#         {"json.return_value": {}, "status_code": 429},
#         {"json.return_value": {}, "status_code": 429},
#         {"json.return_value": {"data": [3], "nextPageToken": "asfg"}, "status_code": 200},
#         {"json.return_value": {}, "status_code": 429},
#         {"json.return_value": {"data": [54], "nextPageToken": None}, "status_code": 200},
#     ]
#     with patch("requests.get") as mock_get, patch(
#         "bfb_delivery.lib.dispatch.api_callers.sleep"
#     ) as mock_sleep:
#         mock_get.side_effect = [Mock(**resp) for resp in responses]

#         _ = get_responses(BASE_URL)

#         expected_sleep_calls = []
#         wait_time = RateLimits.READ_SECONDS
#         for resp in responses:
#             if resp["status_code"] == 429:
#                 wait_time *= 2
#             expected_sleep_calls.append(wait_time)

#         actual_sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]

#         assert actual_sleep_calls == expected_sleep_calls


@typechecked
def test_get_circuit_key(FAKE_KEY: str) -> None:
    """Test get_circuit_key function."""
    assert get_circuit_key() == FAKE_KEY
