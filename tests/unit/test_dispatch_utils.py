"""Test for the dispatch utils module."""

from contextlib import AbstractContextManager, nullcontext
from typing import Any
from unittest.mock import patch

import pytest
import requests
import requests_mock

from bfb_delivery.lib.dispatch.utils import get_responses


@pytest.mark.parametrize(
    "responses, expected_result, error_context",
    [
        (
            [{"json": {"data": [1, 2, 3], "nextPageToken": None}, "status_code": 200}],
            [{"data": [1, 2, 3], "nextPageToken": None}],
            nullcontext(),
        ),
        (
            [
                {"json": {"data": [1], "nextPageToken": "abc"}, "status_code": 200},
                {"json": {"data": [2], "nextPageToken": None}, "status_code": 200},
            ],
            [{"data": [1], "nextPageToken": "abc"}, {"data": [2], "nextPageToken": None}],
            nullcontext(),
        ),
        (
            [
                {"json": {}, "status_code": 429},
                {
                    "json.return_value": {"data": [3], "nextPageToken": None},
                    "status_code": 200,
                },
            ],
            [{"data": [3], "nextPageToken": None}],
            nullcontext(),
        ),
        (
            [{"json": {}, "status_code": 400}],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [{"json": {}, "status_code": 401}],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [{"json": {}, "status_code": 403}],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [{"json": {}, "status_code": 404}],
            None,
            pytest.raises(requests.exceptions.HTTPError),
        ),
        (
            [{"json": {}, "status_code": 500}],
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
    url = "https://fakeapi.com/data"

    with patch("requests.get") as mock_get, requests_mock.Mocker() as m:
        side_effects = []
        for resp in responses:
            side_effects.append(
                m.get(
                    f"{url}?{resp.get('nextPageToken') if resp.get('nextPageToken') else ''}",
                    **resp,
                )
            )
        mock_get.side_effect = side_effects

        with error_context:
            result = get_responses(url=url)

        if expected_result:
            assert result == expected_result

        assert mock_get.call_count == len(responses)
