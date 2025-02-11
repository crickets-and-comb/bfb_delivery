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

from bfb_delivery.lib.constants import CIRCUIT_DRIVERS_URL, CircuitColumns
from bfb_delivery.lib.dispatch.write_to_circuit import _get_all_drivers


@pytest.fixture
def mock_all_drivers_simple(requests_mock: Mocker) -> Iterator[Mocker]:  # noqa: F811
    """Mock the Circuit API to return a simple list of drivers."""
    next_page_token = "token123"
    first_url = CIRCUIT_DRIVERS_URL
    second_url = CIRCUIT_DRIVERS_URL + f"?pageToken={next_page_token}"

    first_response = {
        "drivers": [
            {
                CircuitColumns.ID: "driver1",
                CircuitColumns.NAME: "Test Driver1",
                CircuitColumns.EMAIL: "test1@example.com",
                CircuitColumns.ACTIVE: True,
            },
            {
                CircuitColumns.ID: "driverA",
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
                CircuitColumns.ID: "driver2",
                CircuitColumns.NAME: "Another Driver2",
                CircuitColumns.EMAIL: "another2@example.com",
                CircuitColumns.ACTIVE: True,
            },
            {
                CircuitColumns.ID: "driverB",
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


@typechecked
def test_get_all_drivers(mock_all_drivers_simple: Mocker) -> None:
    """Test that _get_all_drivers retrieves all drivers from the Circuit API."""
    drivers_df = _get_all_drivers()

    pd.testing.assert_frame_equal(
        drivers_df.sort_values(by=drivers_df.columns.tolist()).reset_index(drop=True),
        pd.DataFrame(
            {
                CircuitColumns.ID: ["driver1", "driverA", "driver2", "driverB"],
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
        .sort_values(by=drivers_df.columns.tolist())
        .reset_index(drop=True),
    )

    pd.testing.assert_frame_equal(
        drivers_df, drivers_df.sort_values(by=CircuitColumns.NAME).reset_index(drop=True)
    )
