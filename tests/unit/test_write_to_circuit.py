"""Test suite for write_to_circuit.py."""

# NOTE: We're ignoring the top-level `build_routes_from_chunked` since everything it wraps is
# already under test, except the parts defined in the write_to_circuit module itself, i.e.
# `upload_split_chunked`.


# TODO: Mock data from existing fixtures and tools.
# TODO: Mock _make_call for each api_caller, including error responses.
# - Mock 2 pages from PagedResponseGetter._make_call for _get_all_drivers get_responses call.
# TODO: Mock user inputs.
# TODO: Call upload_split_chunked, and check outputs, called withs, raises.
# TODO: Use schema in typehints where applicable.

import builtins
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest
import requests_mock  # noqa: F401
from requests_mock import Mocker
from typeguard import typechecked

from bfb_delivery import split_chunked_route
from bfb_delivery.lib.constants import (
    CIRCUIT_DRIVERS_URL,
    CircuitColumns,
    Columns,
    IntermediateColumns,
)
from bfb_delivery.lib.dispatch.write_to_circuit import (
    _assign_drivers,
    _create_stops_df,
    _get_all_drivers,
)


@pytest.fixture
def mock_driver_df(mock_chunked_sheet_raw: Path) -> pd.DataFrame:
    """Return a DataFrame of drivers."""
    chunked_sheet = pd.read_excel(mock_chunked_sheet_raw)
    driver_df = pd.DataFrame(
        columns=[
            CircuitColumns.ID,
            CircuitColumns.NAME,
            CircuitColumns.EMAIL,
            CircuitColumns.ACTIVE,
        ]
    )
    driver_df[CircuitColumns.NAME] = list(
        set([driver.split("#")[0].strip() for driver in chunked_sheet[Columns.DRIVER]])
    )
    driver_df[CircuitColumns.ID] = driver_df[CircuitColumns.NAME].apply(
        lambda name: f"drivers/{name.replace(' ', '')}"
    )
    driver_df[CircuitColumns.EMAIL] = driver_df[CircuitColumns.NAME].apply(
        lambda name: f"{name.replace(' ', '')}@example.com"
    )
    driver_df[CircuitColumns.ACTIVE] = True

    driver_df = driver_df.sort_values(by=CircuitColumns.NAME).reset_index(drop=True)

    return driver_df


@pytest.fixture
def mock_all_drivers_simple(
    mock_driver_df: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> Iterator[Mocker]:
    """Mock the Circuit API to return a simple list of drivers."""
    drivers_array = mock_driver_df.to_dict(orient="records")
    next_page_token = "token123"

    requests_mock.get(
        url=CIRCUIT_DRIVERS_URL,
        json={
            "drivers": drivers_array[0 : len(drivers_array) // 2],  # noqa: E203
            "nextPageToken": next_page_token,
        },
    )
    requests_mock.get(
        url=CIRCUIT_DRIVERS_URL + f"?pageToken={next_page_token}",
        json={
            "drivers": drivers_array[
                len(drivers_array) // 2 : len(drivers_array)  # noqa: E203
            ],
            "nextPageToken": None,
        },
    )

    yield requests_mock


@pytest.fixture
def mock_split_chunked_sheet(mock_chunked_sheet_raw: Path, tmp_path: Path) -> Path:
    """Create and save a mock split chunked sheet."""
    path = split_chunked_route(
        input_path=mock_chunked_sheet_raw,
        output_dir=tmp_path,
        output_filename="test_split_chunked.xlsx",
        n_books=1,
    )

    return path[0]


@pytest.fixture
def mock_stops_df(mock_split_chunked_sheet: Path, tmp_path: Path) -> pd.DataFrame:
    """Return a mock stops DataFrame."""
    return _create_stops_df(
        split_chunked_workbook_fp=mock_split_chunked_sheet,
        stops_df_path=tmp_path / "stops_df.csv",
    )


@pytest.fixture
def mock_plan_df_initial(mock_stops_df: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame initialized with just the route titles."""
    return pd.DataFrame(
        {
            IntermediateColumns.ROUTE_TITLE: mock_stops_df[
                IntermediateColumns.SHEET_NAME
            ].unique(),
            IntermediateColumns.DRIVER_NAME: None,
            CircuitColumns.EMAIL: None,
            CircuitColumns.ID: None,
        }
    )


@pytest.fixture
@typechecked
def mock_plan_df_drivers_assigned(
    mock_plan_df_initial: pd.DataFrame, mock_driver_df: pd.DataFrame
) -> pd.DataFrame:
    """Return a mock plan DataFrame with drivers assigned."""
    plan_df = mock_plan_df_initial.copy()[[IntermediateColumns.ROUTE_TITLE]]
    plan_df[IntermediateColumns.DRIVER_NAME] = [
        mock_driver_df[
            mock_driver_df[CircuitColumns.NAME].apply(lambda x: x in title)  # noqa: B023
        ][CircuitColumns.NAME].iloc[0]
        for title in plan_df[IntermediateColumns.ROUTE_TITLE]
    ]
    plan_df = plan_df.merge(
        mock_driver_df,
        left_on=IntermediateColumns.DRIVER_NAME,
        right_on=CircuitColumns.NAME,
        how="left",
    )
    plan_df = plan_df[mock_plan_df_initial.columns.to_list() + [CircuitColumns.ACTIVE]]

    return plan_df


@pytest.fixture
@typechecked
def mock_driver_assignment(
    mock_stops_df: pd.DataFrame, mock_driver_df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mock user inputs for driver selection."""
    driver_selections = []
    for sheet_name in sorted(mock_stops_df[IntermediateColumns.SHEET_NAME].unique()):
        driver_row = mock_driver_df[
            mock_driver_df[CircuitColumns.NAME].apply(lambda x: x in sheet_name)  # noqa: B023
        ]
        driver_selections.append(f"{driver_row.index[0] + 1}")

    inputs = iter(driver_selections + ["y"])

    original_input = builtins.input

    def fake_input(prompt: str) -> str:
        if prompt.strip() == "(Pdb)":
            return original_input(prompt)
        return next(inputs)

    monkeypatch.setattr("builtins.input", fake_input)


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
# - Invalid inputs. (Just put them right in the middle of the list?)
# - Retry if confirm.lower() != "y"
def test_assign_drivers(
    mock_plan_df_initial: pd.DataFrame,
    mock_driver_df: pd.DataFrame,
    mock_plan_df_drivers_assigned: pd.DataFrame,
    mock_driver_assignment: Mocker,
) -> None:
    """Test that _assign_drivers assigns drivers to routes correctly."""
    result_df = _assign_drivers(drivers_df=mock_driver_df, plan_df=mock_plan_df_initial)

    result_df[CircuitColumns.ACTIVE] = result_df[CircuitColumns.ACTIVE].astype(bool)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_drivers_assigned)
