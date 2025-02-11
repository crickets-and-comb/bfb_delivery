"""A test suite for the API callers module."""

from contextlib import AbstractContextManager, nullcontext
from typing import Any, Final
from unittest.mock import Mock, patch

import pytest
import requests

from bfb_delivery.lib.constants import RateLimits
from bfb_delivery.lib.dispatch.api_callers import (
    BaseCaller,
    BaseDeleteCaller,
    BaseGetCaller,
    BasePostCaller,
    OptimizationChecker,
    OptimizationLauncher,
)

_CALLER_DICT: Final[dict[str, type[BaseCaller]]] = {
    "get": BaseGetCaller,
    "post": BasePostCaller,
    "delete": BaseDeleteCaller,
    "opt_launcher": OptimizationLauncher,
    "opt_checker": OptimizationChecker,
}
_REQUEST_METHOD_DICT: Final[dict[str, str]] = {
    "get": "get",
    "post": "post",
    "delete": "delete",
    "opt_launcher": "post",
    "opt_checker": "get",
}

_CALLER_KWARGS_DICT: Final[dict[str, dict[str, Any]]] = {
    "opt_launcher": {"plan_id": "shrsrtb", "plan_title": "Mock plan title"},
    "opt_checker": {
        "plan_id": "shrsrtb",
        "plan_title": "Mock plan title",
        "operation_id": "tfhnrtyn",
    },
}
_OPT_LAUNCHER_JSON_200: Final[dict[str, Any]] = {
    "metadata": {"canceled": False},
    "id": "sdfhsth",
    "done": True,
}
_OPT_CHECKER_JSON_200: Final[dict[str, Any]] = {
    "metadata": {"canceled": False},
    "id": "sdfhsth",
    "done": True,
}


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
                    "status_code": 598,
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
                    "status_code": 598,
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
def test_base_caller_response_handling(
    request_type: str,
    response_sequence: list[dict[str, Any]],
    expected_result: dict[str, Any] | None,
    error_context: AbstractContextManager,
) -> None:
    """Test `call_api` handling of different HTTP responses, including retries."""

    class MockCaller(_CALLER_DICT[request_type]):
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

                if any(resp["status_code"] == 598 for resp in response_sequence):
                    spy_handle_timeout.assert_called_once()

                assert mock_request.call_count == len(response_sequence)


@pytest.mark.parametrize(
    "request_type, response_sequence, expected_wait_time",
    [
        (
            "get",
            [{"status_code": 200, "raise_for_status.side_effect": None}],
            RateLimits.READ_SECONDS,
        ),
        (
            "post",
            [{"status_code": 200, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_SECONDS,
        ),
        (
            "delete",
            [{"status_code": 200, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_SECONDS,
        ),
        (
            "opt_launcher",
            [
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_LAUNCHER_JSON_200,
                }
            ],
            RateLimits.OPTIMIZATION_PER_SECOND,
        ),
        (
            "opt_checker",
            [
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_CHECKER_JSON_200,
                }
            ],
            RateLimits.READ_SECONDS,
        ),
        (
            "get",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.READ_SECONDS,
        ),
        (
            "post",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_SECONDS,
        ),
        (
            "delete",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_SECONDS,
        ),
        (
            "opt_launcher",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.OPTIMIZATION_PER_SECOND,
        ),
        (
            "opt_checker",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.READ_SECONDS,
        ),
        (
            "get",
            [
                {
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {"status_code": 200, "raise_for_status.side_effect": None},
            ],
            RateLimits.READ_SECONDS
            * RateLimits.WAIT_INCREASE_SCALAR
            * RateLimits.WAIT_DECREASE_SECONDS,
        ),
        (
            "post",
            [
                {
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {"status_code": 200, "raise_for_status.side_effect": None},
            ],
            RateLimits.WRITE_SECONDS
            * RateLimits.WAIT_INCREASE_SCALAR
            * RateLimits.WAIT_DECREASE_SECONDS,
        ),
        (
            "delete",
            [
                {
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {"status_code": 200, "raise_for_status.side_effect": None},
            ],
            RateLimits.WRITE_SECONDS
            * RateLimits.WAIT_INCREASE_SCALAR
            * RateLimits.WAIT_DECREASE_SECONDS,
        ),
        (
            "opt_launcher",
            [
                {
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_LAUNCHER_JSON_200,
                },
            ],
            RateLimits.OPTIMIZATION_PER_SECOND
            * RateLimits.WAIT_INCREASE_SCALAR
            * RateLimits.WAIT_DECREASE_SECONDS,
        ),
        (
            "opt_checker",
            [
                {
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_CHECKER_JSON_200,
                },
            ],
            RateLimits.READ_SECONDS
            * RateLimits.WAIT_INCREASE_SCALAR
            * RateLimits.WAIT_DECREASE_SECONDS,
        ),
    ],
)
def test_base_caller_wait_time_adjusting(
    request_type: str, response_sequence: list[dict[str, Any]], expected_wait_time: float
) -> None:
    """Test request wait time adjustments on rate-limiting."""

    class MockCaller(_CALLER_DICT[request_type]):
        """Minimal concrete subclass of BaseCaller for testing."""

        def _set_url(self) -> None:
            """Set a dummy test URL."""
            self._url = "https://example.com/api/test"

    with patch(f"requests.{_REQUEST_METHOD_DICT[request_type]}") as mock_request, patch(
        "bfb_delivery.lib.dispatch.api_callers.sleep"
    ):
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]

        mock_caller = MockCaller(**_CALLER_KWARGS_DICT.get(request_type, {}))
        mock_caller.call_api()

        assert MockCaller._wait_seconds == expected_wait_time


@pytest.mark.parametrize(
    "request_type, response_sequence, expected_timeout",
    [
        (
            "get",
            [{"status_code": 200, "raise_for_status.side_effect": None}],
            RateLimits.READ_TIMEOUT_SECONDS,
        ),
        (
            "post",
            [{"status_code": 200, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_TIMEOUT_SECONDS,
        ),
        (
            "delete",
            [{"status_code": 200, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_TIMEOUT_SECONDS,
        ),
        (
            "opt_launcher",
            [
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_LAUNCHER_JSON_200,
                }
            ],
            RateLimits.WRITE_TIMEOUT_SECONDS,
        ),
        (
            "opt_checker",
            [
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_CHECKER_JSON_200,
                }
            ],
            RateLimits.READ_TIMEOUT_SECONDS,
        ),
        (
            "get",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.READ_TIMEOUT_SECONDS,
        ),
        (
            "post",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_TIMEOUT_SECONDS,
        ),
        (
            "delete",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_TIMEOUT_SECONDS,
        ),
        (
            "opt_launcher",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.WRITE_TIMEOUT_SECONDS,
        ),
        (
            "opt_checker",
            [{"status_code": 204, "raise_for_status.side_effect": None}],
            RateLimits.READ_TIMEOUT_SECONDS,
        ),
        (
            "get",
            [
                {
                    "status_code": 598,
                    "raise_for_status.side_effect": requests.exceptions.Timeout,
                },
                {"status_code": 200, "raise_for_status.side_effect": None},
            ],
            RateLimits.READ_TIMEOUT_SECONDS * RateLimits.WAIT_INCREASE_SCALAR,
        ),
        (
            "post",
            [
                {
                    "status_code": 598,
                    "raise_for_status.side_effect": requests.exceptions.Timeout,
                },
                {"status_code": 200, "raise_for_status.side_effect": None},
            ],
            RateLimits.WRITE_TIMEOUT_SECONDS * RateLimits.WAIT_INCREASE_SCALAR,
        ),
        (
            "delete",
            [
                {
                    "status_code": 598,
                    "raise_for_status.side_effect": requests.exceptions.Timeout,
                },
                {"status_code": 200, "raise_for_status.side_effect": None},
            ],
            RateLimits.WRITE_TIMEOUT_SECONDS * RateLimits.WAIT_INCREASE_SCALAR,
        ),
        (
            "opt_launcher",
            [
                {
                    "status_code": 598,
                    "raise_for_status.side_effect": requests.exceptions.Timeout,
                },
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_LAUNCHER_JSON_200,
                },
            ],
            RateLimits.WRITE_TIMEOUT_SECONDS * RateLimits.WAIT_INCREASE_SCALAR,
        ),
        (
            "opt_checker",
            [
                {
                    "status_code": 598,
                    "raise_for_status.side_effect": requests.exceptions.Timeout,
                },
                {
                    "status_code": 200,
                    "raise_for_status.side_effect": None,
                    "json.return_value": _OPT_CHECKER_JSON_200,
                },
            ],
            RateLimits.READ_TIMEOUT_SECONDS * RateLimits.WAIT_INCREASE_SCALAR,
        ),
    ],
)
def test_base_caller_timeout_adjusting(
    request_type: str, response_sequence: list[dict[str, Any]], expected_timeout: float
) -> None:
    """Test timeout adjustment on timeout retry."""

    class MockCaller(_CALLER_DICT[request_type]):
        """Minimal concrete subclass of BaseCaller for testing."""

        def _set_url(self) -> None:
            """Set a dummy test URL."""
            self._url = "https://example.com/api/test"

    with patch(f"requests.{_REQUEST_METHOD_DICT[request_type]}") as mock_request, patch(
        "bfb_delivery.lib.dispatch.api_callers.sleep"
    ):
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]

        mock_caller = MockCaller(**_CALLER_KWARGS_DICT.get(request_type, {}))
        mock_caller.call_api()

        assert MockCaller._timeout == expected_timeout
