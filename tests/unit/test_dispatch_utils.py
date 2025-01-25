"""Test for the dispatch utils module."""

from contextlib import AbstractContextManager, nullcontext
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests

from bfb_delivery.lib.dispatch.utils import get_responses


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
                {
                    "json.return_value": {"data": [3], "nextPageToken": None},
                    "status_code": 200,
                },
            ],
            [{"data": [3], "nextPageToken": None}],
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
def test_get_responses(
    responses: list[dict[str, Any]],
    expected_result: list[dict[str, Any]],
    error_context: AbstractContextManager,
) -> None:
    """Test get_responses function."""
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [Mock(**resp) for resp in responses]

        with error_context:
            result = get_responses("https://fakeapi.com/data")

        if expected_result:
            assert result == expected_result

        assert mock_get.call_count == len(responses)
