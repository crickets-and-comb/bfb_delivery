"""A test suite for the API callers module."""

from contextlib import AbstractContextManager, nullcontext
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests

from bfb_delivery.lib.dispatch.api_callers import (
    BaseDeleteCaller,
    BaseGetCaller,
    BasePostCaller,
)


@pytest.mark.parametrize("request_type", ["get", "post", "delete"])
@pytest.mark.parametrize(
    "response_sequence, expected_result, error_context",
    [
        (
            [
                {
                    "json.return_value": {"data": [1, 2, 3]},
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                }
            ],
            {"data": [1, 2, 3]},
            nullcontext(),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 204,
                    "raise_for_status.side_effect": None,
                }
            ],
            {},
            nullcontext(),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {
                    "json.return_value": {"data": [5, 6]},
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                },
            ],
            {"data": [5, 6]},
            nullcontext(),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 443,
                    "raise_for_status.side_effect": requests.exceptions.Timeout,
                },
                {
                    "json.return_value": {"data": [7, 8]},
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                },
            ],
            {"data": [7, 8]},
            nullcontext(),
        ),
        (
            [
                {
                    "status_code": 400,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                }
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError, match="Got 400 response"),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {
                    "status_code": 400,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError, match="Got 400 response"),
        ),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 443,
                    "raise_for_status.side_effect": requests.exceptions.Timeout,
                },
                {
                    "status_code": 400,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
            ],
            None,
            pytest.raises(requests.exceptions.HTTPError, match="Got 400 response"),
        ),
    ],
)
def test_get_caller(
    request_type: str,
    response_sequence: list[dict[str, Any]],
    expected_result: dict[str, Any] | None,
    error_context: AbstractContextManager,
) -> None:
    """Test `call_api` handling of different HTTP responses, including retries."""
    caller_dict = {"get": BaseGetCaller, "post": BasePostCaller, "delete": BaseDeleteCaller}

    class MockCaller(caller_dict[request_type]):
        """Minimal concrete subclass of BaseCaller for testing."""

        def _set_url(self) -> None:
            """Set a dummy test URL."""
            self._url = "https://example.com/api/test"

    with patch(f"requests.{request_type}") as mock_request:
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]
        mock_caller = MockCaller()

        with patch("bfb_delivery.lib.dispatch.api_callers.sleep"), patch.object(
            mock_caller, "_handle_429", wraps=mock_caller._handle_429
        ) as spy_handle_429, patch.object(
            mock_caller, "_handle_timeout", wraps=mock_caller._handle_timeout
        ) as spy_handle_timeout:

            with error_context:
                mock_caller.call_api()

            if isinstance(error_context, nullcontext):
                assert mock_caller.response_json == expected_result

                if any(resp["status_code"] == 429 for resp in response_sequence):
                    spy_handle_429.assert_called_once()

                if any(resp["status_code"] == 443 for resp in response_sequence):
                    spy_handle_timeout.assert_called_once()

                assert mock_request.call_count == len(response_sequence)
