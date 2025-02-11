"""A test suite for the API callers module."""

import re
from collections.abc import Iterator
from contextlib import AbstractContextManager, nullcontext
from typing import Any, Final
from unittest.mock import Mock, patch

import pytest
import requests
from typeguard import typechecked

from bfb_delivery.lib.constants import CIRCUIT_URL, CircuitColumns, RateLimits
from bfb_delivery.lib.dispatch.api_callers import (
    BaseCaller,
    BaseDeleteCaller,
    BaseGetCaller,
    BasePostCaller,
    OptimizationChecker,
    OptimizationLauncher,
    PagedResponseGetter,
    PlanInitializer,
    StopUploader,
)

_MOCK_OPERATION_ID: Final[str] = "sdfhsth"
_MOCK_PLAN_ID: Final[str] = "shrsrtb"
_MOCK_PLAN_TITLE: Final[str] = "Mock plan title"
_MOCK_STOP_ARRAY: Final[list[dict[str, dict[str, str] | list[str] | int | str]]] = [
    {"adfbssd": "asfga"},
    {"sgdbsb": "sfdgsg"},
]

_CALLER_DICT: Final[dict[str, type[BaseCaller]]] = {
    "get": BaseGetCaller,
    "post": BasePostCaller,
    "delete": BaseDeleteCaller,
    "opt_launcher": OptimizationLauncher,
    "opt_checker": OptimizationChecker,
    "stop_uploader": StopUploader,
}
_REQUEST_METHOD_DICT: Final[dict[str, str]] = {
    "get": "get",
    "post": "post",
    "delete": "delete",
    "opt_launcher": "post",
    "opt_checker": "get",
    "stop_uploader": "post",
}
_CALLER_KWARGS_DICT: Final[dict[str, dict[str, Any]]] = {
    "opt_launcher": {"plan_id": _MOCK_PLAN_ID, "plan_title": _MOCK_PLAN_TITLE},
    "opt_checker": {
        "plan_id": _MOCK_PLAN_ID,
        "plan_title": _MOCK_PLAN_TITLE,
        "operation_id": _MOCK_OPERATION_ID,
    },
    "stop_uploader": {
        "plan_id": _MOCK_PLAN_ID,
        "plan_title": _MOCK_PLAN_TITLE,
        "stop_array": _MOCK_STOP_ARRAY,
    },
}

_OPT_JSON_200_DONE: Final[dict[str, Any]] = {
    "metadata": {"canceled": False},
    "result": {},
    "id": _MOCK_OPERATION_ID,
    "done": True,
}
_OPT_JSON_200_UNFINISHED: Final[dict[str, Any]] = {
    "metadata": {"canceled": False},
    "result": {},
    "id": _MOCK_OPERATION_ID,
    "done": False,
}
_OPT_JSON_CANCELED: dict[str, Any] = _OPT_JSON_200_DONE.copy()
_OPT_JSON_CANCELED.update({"metadata": {"canceled": True}})
_OPT_JSON_SKIPPED: dict[str, Any] = _OPT_JSON_200_DONE.copy()
_OPT_JSON_SKIPPED["result"] = {"skippedStops": [1, 2, 3]}
_OPT_JSON_ERROR_CODE: dict[str, Any] = _OPT_JSON_200_DONE.copy()
_OPT_JSON_ERROR_CODE["result"] = {"code": "MOCK_ERROR_CODE"}


@pytest.fixture(autouse=True)
@typechecked
def mock_sleep() -> Iterator[None]:
    """Mock `time.sleep` to avoid waiting in tests."""
    with patch("bfb_delivery.lib.dispatch.api_callers.sleep"):
        yield


@pytest.mark.parametrize("request_type", ["get", "post", "delete"])
@pytest.mark.parametrize(
    "response_sequence, expected_result, error_context",
    [
        (
            [{"json.return_value": {"data": [1, 2, 3]}, "status_code": 200}],
            {"data": [1, 2, 3]},
            nullcontext(),
        ),
        ([{"json.return_value": {}, "status_code": 204}], {}, nullcontext()),
        (
            [
                {
                    "json.return_value": {},
                    "status_code": 429,
                    "raise_for_status.side_effect": requests.exceptions.HTTPError,
                },
                {"json.return_value": {"data": [5, 6]}, "status_code": 200},
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
                {"json.return_value": {"data": [7, 8]}, "status_code": 200},
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
@typechecked
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

        with patch.object(
            mock_caller, "_handle_429", wraps=mock_caller._handle_429
        ) as spy_handle_429, patch.object(
            mock_caller, "_handle_timeout", wraps=mock_caller._handle_timeout
        ) as spy_handle_timeout:

            with error_context:
                mock_caller.call_api()

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
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_DONE}],
            RateLimits.OPTIMIZATION_PER_SECOND,
        ),
        (
            "opt_checker",
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_DONE}],
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
                {"status_code": 200, "json.return_value": _OPT_JSON_200_DONE},
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
                {"status_code": 200, "json.return_value": _OPT_JSON_200_DONE},
            ],
            RateLimits.READ_SECONDS
            * RateLimits.WAIT_INCREASE_SCALAR
            * RateLimits.WAIT_DECREASE_SECONDS,
        ),
    ],
)
@typechecked
def test_base_caller_wait_time_adjusting(
    request_type: str, response_sequence: list[dict[str, Any]], expected_wait_time: float
) -> None:
    """Test request wait time adjustments on rate-limiting."""

    class MockCaller(_CALLER_DICT[request_type]):
        """Minimal concrete subclass of BaseCaller for testing."""

        def _set_url(self) -> None:
            """Set a dummy test URL."""
            self._url = "https://example.com/api/test"

    with patch(f"requests.{_REQUEST_METHOD_DICT[request_type]}") as mock_request:
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
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_DONE}],
            RateLimits.WRITE_TIMEOUT_SECONDS,
        ),
        (
            "opt_checker",
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_DONE}],
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
                {"status_code": 200, "json.return_value": _OPT_JSON_200_DONE},
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
                {"status_code": 200, "json.return_value": _OPT_JSON_200_DONE},
            ],
            RateLimits.READ_TIMEOUT_SECONDS * RateLimits.WAIT_INCREASE_SCALAR,
        ),
    ],
)
@typechecked
def test_base_caller_timeout_adjusting(
    request_type: str, response_sequence: list[dict[str, Any]], expected_timeout: float
) -> None:
    """Test timeout adjustment on timeout retry."""

    class MockCaller(_CALLER_DICT[request_type]):
        """Minimal concrete subclass of BaseCaller for testing."""

        def _set_url(self) -> None:
            """Set a dummy test URL."""
            self._url = "https://example.com/api/test"

    with patch(f"requests.{_REQUEST_METHOD_DICT[request_type]}") as mock_request:
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]

        mock_caller = MockCaller(**_CALLER_KWARGS_DICT.get(request_type, {}))
        mock_caller.call_api()

        assert MockCaller._timeout == expected_timeout


@pytest.mark.parametrize(
    "request_type, response_sequence, expected_done_status, error_context",
    [
        (
            "opt_launcher",
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_DONE}],
            True,
            nullcontext(),
        ),
        (
            "opt_checker",
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_DONE}],
            True,
            nullcontext(),
        ),
        (
            "opt_launcher",
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_UNFINISHED}],
            False,
            nullcontext(),
        ),
        (
            "opt_checker",
            [{"status_code": 200, "json.return_value": _OPT_JSON_200_UNFINISHED}],
            False,
            nullcontext(),
        ),
        (
            "opt_launcher",
            [{"status_code": 200, "json.return_value": _OPT_JSON_CANCELED}],
            None,
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"Optimization canceled for {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}):"
                ),
            ),
        ),
        (
            "opt_checker",
            [{"status_code": 200, "json.return_value": _OPT_JSON_CANCELED}],
            None,
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"Optimization canceled for {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}):"
                ),
            ),
        ),
        (
            "opt_launcher",
            [{"status_code": 200, "json.return_value": _OPT_JSON_SKIPPED}],
            None,
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"Skipped optimization stops for {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}):"
                ),
            ),
        ),
        (
            "opt_checker",
            [{"status_code": 200, "json.return_value": _OPT_JSON_SKIPPED}],
            None,
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"Skipped optimization stops for {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}):"
                ),
            ),
        ),
        (
            "opt_launcher",
            [{"status_code": 200, "json.return_value": _OPT_JSON_ERROR_CODE}],
            None,
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"Errors in optimization for {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}):"
                ),
            ),
        ),
        (
            "opt_checker",
            [{"status_code": 200, "json.return_value": _OPT_JSON_ERROR_CODE}],
            None,
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"Errors in optimization for {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}):"
                ),
            ),
        ),
    ],
)
@typechecked
def test_optimization_callers(
    request_type: str,
    response_sequence: list[dict[str, Any]],
    expected_done_status: bool | None,
    error_context: AbstractContextManager,
) -> None:
    """Test optimization callers."""
    with patch(f"requests.{_REQUEST_METHOD_DICT[request_type]}") as mock_request:
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]
        kwargs = _CALLER_KWARGS_DICT.get(request_type, {})
        caller = (
            OptimizationLauncher(**kwargs)
            if request_type == "opt_launcher"
            else OptimizationChecker(**kwargs)
        )
        if request_type == "opt_checker":
            assert caller.operation_id == _MOCK_OPERATION_ID

        with error_context:
            caller.call_api()
            assert (
                mock_request.call_args_list[0][1]["url"]
                == f"{CIRCUIT_URL}/{kwargs["plan_id"]}:optimize"
                if request_type == "opt_launcher"
                else f"{CIRCUIT_URL}{kwargs["operation_id"]}"
            )
            assert caller.operation_id == _MOCK_OPERATION_ID
            assert caller.finished == expected_done_status


@pytest.mark.parametrize(
    "response_sequence",
    [
        ([{"status_code": 200, "json.return_value": {"nextPageToken": "abc123"}}]),
        ([{"status_code": 200, "json.return_value": {"nextPageToken": None}}]),
        ([{"status_code": 200, "json.return_value": {}}]),
    ],
)
@typechecked
def test_paged_getter(response_sequence: list[dict[str, Any]]) -> None:
    """Test PagedResponseGetter."""
    with patch("requests.get") as mock_request:
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]

        page_url = "https://example.com/api/test"
        caller = PagedResponseGetter(page_url=page_url)
        caller.call_api()
        assert mock_request.call_args_list[0][1]["url"] == page_url
        assert caller.next_page_salsa == response_sequence[-1]["json.return_value"].get(
            "nextPageToken", None
        )


@pytest.mark.parametrize(
    "response_sequence",
    [
        (
            [
                {
                    "status_code": 200,
                    "json.return_value": {
                        CircuitColumns.ID: _MOCK_PLAN_ID,
                        CircuitColumns.WRITABLE: True,
                    },
                }
            ]
        ),
        (
            [
                {
                    "status_code": 200,
                    "json.return_value": {
                        CircuitColumns.ID: _MOCK_PLAN_ID,
                        CircuitColumns.WRITABLE: False,
                    },
                }
            ]
        ),
    ],
)
@typechecked
def test_plan_initializer(response_sequence: list[dict[str, Any]]) -> None:
    """Test PlanInitializer."""
    with patch("requests.post") as mock_request:
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]

        plan_data = {"mock_item": "mock_value"}
        caller = PlanInitializer(plan_data=plan_data)
        caller.call_api()

        assert mock_request.call_args_list[0][1]["url"] == f"{CIRCUIT_URL}/plans"
        assert mock_request.call_args_list[0][1]["json"] == plan_data
        assert caller.plan_id == _MOCK_PLAN_ID
        assert (
            caller.writable
            == response_sequence[0]["json.return_value"][CircuitColumns.WRITABLE]
        )


@pytest.mark.parametrize(
    "response_sequence, error_context",
    [
        (
            [
                {
                    "status_code": 200,
                    "json.return_value": {
                        CircuitColumns.ID: _MOCK_PLAN_ID,
                        "success": [1, 2],
                        "failed": [],
                    },
                }
            ],
            nullcontext(),
        ),
        (
            [
                {
                    "status_code": 200,
                    "json.return_value": {
                        CircuitColumns.ID: _MOCK_PLAN_ID,
                        "success": [1],
                        "failed": [{"error": "mock error"}],
                    },
                }
            ],
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"For {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}), failed to upload stops:"
                ),
            ),
        ),
        (
            [
                {
                    "status_code": 200,
                    "json.return_value": {
                        CircuitColumns.ID: _MOCK_PLAN_ID,
                        "success": [1],
                        "failed": [],
                    },
                }
            ],
            pytest.raises(
                RuntimeError,
                match=re.escape(
                    f"For {_MOCK_PLAN_TITLE} ({_MOCK_PLAN_ID}), "
                    "did not upload same number of stops as input:"
                ),
            ),
        ),
    ],
)
@typechecked
def test_stop_uploader(
    response_sequence: list[dict[str, Any]], error_context: AbstractContextManager
) -> None:
    """Test PlanInitializer."""
    with patch("requests.post") as mock_request:
        mock_request.side_effect = [Mock(**resp) for resp in response_sequence]

        caller = StopUploader(
            plan_id=_MOCK_PLAN_ID, plan_title=_MOCK_PLAN_TITLE, stop_array=_MOCK_STOP_ARRAY
        )

        with error_context:
            caller.call_api()

            assert (
                mock_request.call_args_list[0][1]["url"]
                == f"{CIRCUIT_URL}/{_MOCK_PLAN_ID}/stops:import"
            )
            assert caller.stop_ids == response_sequence[0]["json.return_value"]["success"]
