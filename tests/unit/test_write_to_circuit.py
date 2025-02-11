"""Test suite for write_to_circuit.py."""

# NOTE: We're ignoring the top-level `build_routes_from_chunked` since everything it wraps is
# already under test, except the parts defined in the write_to_circuit module itself, i.e.
# `upload_split_chunked`.


# TODO: Mock data from existing fixtures and tools.
# TODO: Mock _make_call for each api_caller, including error responses.
# - Mock 2 pages from PagedResponseGetter._make_call for _get_all_drivers get_responses call.
# TODO: Mock user inputs.
# TODO: Call upload_split_chunked, and check outputs, called withs, raises.

from collections.abc import Iterator

import pandas as pd
import pytest
import requests_mock  # noqa: F401
from requests_mock import Mocker
from typeguard import typechecked

from bfb_delivery.lib.constants import (
    CIRCUIT_DRIVERS_URL,
    CircuitColumns,
    IntermediateColumns,
)
from bfb_delivery.lib.dispatch.write_to_circuit import _assign_drivers, _get_all_drivers


@pytest.fixture
def mock_all_drivers_simple(requests_mock: Mocker) -> Iterator[Mocker]:  # noqa: F811
    """Mock the Circuit API to return a simple list of drivers."""
    next_page_token = "token123"
    first_url = CIRCUIT_DRIVERS_URL
    second_url = CIRCUIT_DRIVERS_URL + f"?pageToken={next_page_token}"

    first_response = {
        "drivers": [
            {
                CircuitColumns.ID: "drivers/driver1",
                CircuitColumns.NAME: "Test Driver1",
                CircuitColumns.EMAIL: "test1@example.com",
                CircuitColumns.ACTIVE: True,
            },
            {
                CircuitColumns.ID: "drivers/driverA",
                CircuitColumns.NAME: "Another DriverA",
                CircuitColumns.EMAIL: "anothera@example.com",
                CircuitColumns.ACTIVE: True,
            },
        ],
        "nextPageToken": next_page_token,
    }
    second_response = {
        "drivers": [
            {
                CircuitColumns.ID: "drivers/driver2",
                CircuitColumns.NAME: "Another Driver2",
                CircuitColumns.EMAIL: "another2@example.com",
                CircuitColumns.ACTIVE: True,
            },
            {
                CircuitColumns.ID: "drivers/driverB",
                CircuitColumns.NAME: "Test DriverB",
                CircuitColumns.EMAIL: "testB@example.com",
                CircuitColumns.ACTIVE: True,
            },
        ],
        "nextPageToken": None,
    }

    requests_mock.get(first_url, json=first_response)
    requests_mock.get(second_url, json=second_response)

    yield requests_mock


@pytest.fixture
def mock_driver_df() -> pd.DataFrame:
    """Return a DataFrame of drivers."""
    return pd.DataFrame(
        {
            CircuitColumns.ID: [
                "drivers/driver1",
                "drivers/driverA",
                "drivers/driver2",
                "drivers/driverB",
            ],
            CircuitColumns.NAME: [
                "Test Driver1",
                "Another DriverA",
                "Another Driver2",
                "Test DriverB",
            ],
            CircuitColumns.EMAIL: [
                "test1@example.com",
                "anothera@example.com",
                "another2@example.com",
                "testB@example.com",
            ],
            CircuitColumns.ACTIVE: [True, True, True, True],
        }
    )


@typechecked
def test_get_all_drivers(
    mock_driver_df: pd.DataFrame, mock_all_drivers_simple: Mocker
) -> None:
    """Test that _get_all_drivers retrieves all drivers from the Circuit API."""
    drivers_df = _get_all_drivers()

    pd.testing.assert_frame_equal(
        drivers_df.sort_values(by=drivers_df.columns.tolist()).reset_index(drop=True),
        mock_driver_df.sort_values(by=drivers_df.columns.tolist()).reset_index(drop=True),
    )

    pd.testing.assert_frame_equal(
        drivers_df, drivers_df.sort_values(by=CircuitColumns.NAME).reset_index(drop=True)
    )


# TODO: Test errors etc.:
# - Retry if if confirm.lower() != "y"
# - Invalid inputs.
def test_assign_drivers(
    mock_driver_df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that _assign_drivers assigns drivers to routes correctly."""
    plan_df = pd.DataFrame(
        {
            IntermediateColumns.ROUTE_TITLE: ["Test Driver Route1", "Test Driver Route2"],
            IntermediateColumns.DRIVER_NAME: None,
            CircuitColumns.EMAIL: None,
            CircuitColumns.ID: None,
        }
    )

    inputs = iter(["1", "2", "y"])
    monkeypatch.setattr("builtins.input", lambda prompt: next(inputs))

    result_df = _assign_drivers(drivers_df=mock_driver_df, plan_df=plan_df)
    result_df[CircuitColumns.ACTIVE] = result_df[CircuitColumns.ACTIVE].astype(bool)

    expected_df = pd.concat(
        [
            plan_df[[IntermediateColumns.ROUTE_TITLE]],
            mock_driver_df.iloc[0:2][
                [
                    CircuitColumns.NAME,
                    CircuitColumns.EMAIL,
                    CircuitColumns.ID,
                    CircuitColumns.ACTIVE,
                ]
            ],
        ],
        axis=1,
    ).rename(columns={CircuitColumns.NAME: IntermediateColumns.DRIVER_NAME})

    pd.testing.assert_frame_equal(result_df, expected_df)
