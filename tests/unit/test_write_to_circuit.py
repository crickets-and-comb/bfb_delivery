"""Test suite for write_to_circuit.py."""

# NOTE: We're ignoring the top-level `build_routes_from_chunked` since everything it wraps is
# already under test, except the parts defined in the write_to_circuit module itself, i.e.
# `upload_split_chunked`.

# TODO: Use schema in typehints where applicable.

import builtins
from collections.abc import Iterator
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Any, Final
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import requests_mock  # noqa: F401
from requests import Request
from requests_mock import Mocker
from typeguard import typechecked

from bfb_delivery import split_chunked_route
from bfb_delivery.lib.constants import (
    CIRCUIT_DRIVERS_URL,
    CIRCUIT_URL,
    CircuitColumns,
    Columns,
    IntermediateColumns,
)
from bfb_delivery.lib.dispatch.write_to_circuit import (
    _assign_drivers,
    _assign_drivers_to_plans,
    _build_stop_array,
    _create_plans,
    _create_stops_df,
    _distribute_routes,
    _get_all_drivers,
    _initialize_plans,
    _optimize_routes,
    _parse_addresses,
    _upload_stops,
    delete_plan,
    delete_plans,
    upload_split_chunked,
)

_START_DATE: Final[str] = "2025-01-01"
_DELETE_PLAN_IDS: Final[list[str]] = ["plans/plan1", "plans/plan2", "plans/plan3"]
_DELETE_PLAN_DF: Final[pd.DataFrame] = pd.DataFrame(
    {IntermediateColumns.PLAN_ID: [f"{plan_id}_df" for plan_id in _DELETE_PLAN_IDS]}
)
_FAILURE_IDX: Final[int] = 0


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


@typechecked
def register_driver_gets(
    drivers_array: list[dict], requests_mock: Mocker  # noqa: F811
) -> None:
    """Register GET requests for drivers."""
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


@pytest.fixture
def mock_get_all_drivers(
    mock_driver_df: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock the Circuit API to return a simple list of drivers."""
    register_driver_gets(
        drivers_array=mock_driver_df.to_dict(orient="records"), requests_mock=requests_mock
    )


@pytest.fixture
def mock_get_all_drivers_with_inactive(
    mock_driver_df: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock the Circuit API to return a simple list of drivers."""
    driver_df = mock_driver_df.copy()
    driver_df.loc[_FAILURE_IDX, CircuitColumns.ACTIVE] = False
    register_driver_gets(
        drivers_array=driver_df.to_dict(orient="records"), requests_mock=requests_mock
    )


@pytest.fixture
def mock_stops_df(mock_split_chunked_sheet: Path, tmp_path: Path) -> pd.DataFrame:
    """Return a mock stops DataFrame."""
    # TODO: Build unit test for _create_stops_df.
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
def driver_selections(mock_stops_df: pd.DataFrame, mock_driver_df: pd.DataFrame) -> list[str]:
    """Make generic driver selections for each route."""
    driver_selections = []
    for sheet_name in sorted(mock_stops_df[IntermediateColumns.SHEET_NAME].unique()):
        driver_row = mock_driver_df[
            mock_driver_df[CircuitColumns.NAME].apply(lambda x: x in sheet_name)  # noqa: B023
        ]
        driver_selections.append(f"{driver_row.index[0] + 1}")

    return driver_selections


@typechecked
def patch_user_input(inputs: Iterator[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch user input for driver selection."""
    original_input = builtins.input

    def fake_input(prompt: str) -> str:
        if prompt.strip() == "(Pdb)":
            return original_input(prompt)
        return next(inputs)

    monkeypatch.setattr("builtins.input", fake_input)


@pytest.fixture
@typechecked
def mock_driver_assignment(
    driver_selections: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mock user inputs for driver selection."""
    patch_user_input(inputs=iter(driver_selections + ["y"]), monkeypatch=monkeypatch)


@pytest.fixture
@typechecked
def mock_driver_assignment_with_derps(
    driver_selections: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mock user inputs for driver selection."""
    inputs = iter(
        driver_selections[0 : len(driver_selections) // 2]  # noqa: E203
        + ["derp", "9999"]
        + driver_selections[len(driver_selections) // 2 :]  # noqa: E203
        + ["y"]
    )

    patch_user_input(inputs=inputs, monkeypatch=monkeypatch)


@pytest.fixture
@typechecked
def mock_driver_assignment_with_retry(
    driver_selections: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mock user inputs for driver selection."""
    inputs = iter(["1"] * len(driver_selections) + ["n"] + driver_selections + ["y"])
    patch_user_input(inputs=inputs, monkeypatch=monkeypatch)


@pytest.fixture
@typechecked
def mock_plan_df_plans_initialized(
    mock_plan_df_drivers_assigned: pd.DataFrame,
) -> pd.DataFrame:
    """Return a mock plan DataFrame with drivers assigned."""
    plan_df = mock_plan_df_drivers_assigned.copy()
    plan_df[IntermediateColumns.PLAN_ID] = mock_plan_df_drivers_assigned[
        IntermediateColumns.ROUTE_TITLE
    ].apply(lambda title: f"plans/{title.replace(' ', '').replace("#", '')}")
    plan_df[CircuitColumns.WRITABLE] = True
    # TODO: Do we need this column?
    plan_df[CircuitColumns.OPTIMIZATION] = None
    plan_df[IntermediateColumns.INITIALIZED] = True

    return plan_df


@pytest.fixture
@typechecked
def mock_plan_df_plans_initialized_with_failure(
    mock_plan_df_plans_initialized: pd.DataFrame,
) -> pd.DataFrame:
    """Return a mock plan DataFrame with drivers assigned."""
    plan_df = mock_plan_df_plans_initialized.copy()
    plan_df.loc[_FAILURE_IDX, CircuitColumns.WRITABLE] = False
    plan_df.loc[_FAILURE_IDX, IntermediateColumns.INITIALIZED] = False

    return plan_df


@typechecked
def register_plan_initialization(
    plan_df: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Make responses for plan initialization."""
    responses = {}
    for _, row in plan_df.iterrows():
        title = row[IntermediateColumns.ROUTE_TITLE]
        responses[title] = {
            CircuitColumns.ID: row[IntermediateColumns.PLAN_ID],
            CircuitColumns.WRITABLE: row[CircuitColumns.WRITABLE],
        }

    def post_callback(request: Request, context: Any) -> Any:
        data = request.json()
        plan_title = data.get(CircuitColumns.TITLE)
        return responses[plan_title]

    requests_mock.register_uri(
        "POST", f"{CIRCUIT_URL}/plans", json=post_callback, status_code=200
    )


@pytest.fixture
@typechecked
def mock_plan_initialization(
    mock_plan_df_plans_initialized: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for plan initialization."""
    register_plan_initialization(
        plan_df=mock_plan_df_plans_initialized, requests_mock=requests_mock
    )


@pytest.fixture
@typechecked
def mock_plan_initialization_failure(
    mock_plan_df_plans_initialized_with_failure: pd.DataFrame,
    requests_mock: Mocker,  # noqa: F811
) -> None:
    """Mock requests.post calls for plan initialization."""
    register_plan_initialization(
        plan_df=mock_plan_df_plans_initialized_with_failure, requests_mock=requests_mock
    )


@pytest.fixture
@typechecked
def mock_plan_df_stops_uploaded(mock_plan_df_plans_initialized: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame with stops uploaded."""
    plan_df = mock_plan_df_plans_initialized.copy()
    plan_df[IntermediateColumns.STOPS_UPLOADED] = True

    return plan_df


@pytest.fixture
@typechecked
def mock_plan_df_stops_uploaded_with_error(
    mock_plan_df_stops_uploaded: pd.DataFrame,
) -> pd.DataFrame:
    """Return a mock plan DataFrame with stops uploaded."""
    plan_df = mock_plan_df_stops_uploaded.copy()
    plan_df.loc[_FAILURE_IDX, IntermediateColumns.STOPS_UPLOADED] = False

    return plan_df


@typechecked
def register_stop_upload(
    stops_df: pd.DataFrame, plan_df: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> dict[str, Any]:
    """Register POST requests for stop uploads."""
    stop_arrays = {}
    responses = {}
    for _, row in plan_df.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        title = row[IntermediateColumns.ROUTE_TITLE]

        this_stops_df = stops_df[stops_df[IntermediateColumns.SHEET_NAME] == title]
        # TODO: Build unit test for _parse_addresses.
        this_stops_df = _parse_addresses(stops_df=this_stops_df)

        # TODO: Build unit test for _build_stop_array.
        stop_arrays[plan_id] = _build_stop_array(
            route_stops=this_stops_df, driver_id=row[CircuitColumns.ID]
        )

        responses[plan_id] = (
            {
                "success": [
                    "stops/" + row[Columns.NAME] + row[Columns.PRODUCT_TYPE]
                    for _, row in this_stops_df.iterrows()
                ],
                "failed": [],
            }
            if row[IntermediateColumns.STOPS_UPLOADED]
            else {"success": [], "failed": ["something failed"]}
        )

    for plan_id in responses:
        requests_mock.register_uri(
            "POST",
            f"{CIRCUIT_URL}/{plan_id}/stops:import",
            json=responses[plan_id],
            status_code=200,
        )

    return stop_arrays


@pytest.fixture
@typechecked
def mock_stop_upload(
    mock_plan_df_stops_uploaded: pd.DataFrame,
    mock_stops_df: pd.DataFrame,
    requests_mock: Mocker,  # noqa: F811
) -> dict[str, list]:
    """Mock requests.post calls for stop uploads."""
    return register_stop_upload(
        stops_df=mock_stops_df,
        plan_df=mock_plan_df_stops_uploaded,
        requests_mock=requests_mock,
    )


@pytest.fixture
@typechecked
def mock_stop_upload_failure(
    mock_plan_df_stops_uploaded_with_error: pd.DataFrame,
    mock_stops_df: pd.DataFrame,
    requests_mock: Mocker,  # noqa: F811
) -> dict[str, list]:
    """Mock requests.post calls for stop uploads."""
    return register_stop_upload(
        stops_df=mock_stops_df,
        plan_df=mock_plan_df_stops_uploaded_with_error,
        requests_mock=requests_mock,
    )


@pytest.fixture
@typechecked
def mock_stop_upload_after_failure(
    mock_plan_df_plans_initialized_with_failure: pd.DataFrame,
    mock_stops_df: pd.DataFrame,
    requests_mock: Mocker,  # noqa: F811
) -> dict[str, list]:
    """Mock requests.post calls for stop uploads."""
    plan_df = mock_plan_df_plans_initialized_with_failure.copy()
    plan_df = plan_df[plan_df[IntermediateColumns.INITIALIZED] == True]  # noqa: E712
    plan_df[IntermediateColumns.STOPS_UPLOADED] = True
    stop_arrays = register_stop_upload(
        stops_df=mock_stops_df, plan_df=plan_df, requests_mock=requests_mock
    )

    return stop_arrays


@pytest.fixture
@typechecked
def mock_plan_df_optimized(mock_plan_df_stops_uploaded: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame with optimizations confirmed."""
    plan_df = mock_plan_df_stops_uploaded.copy()
    plan_df[IntermediateColumns.OPTIMIZED] = True
    plan_df[IntermediateColumns.OPTIMIZED] = plan_df[IntermediateColumns.OPTIMIZED].astype(
        object
    )

    return plan_df


@pytest.fixture
@typechecked
def mock_plan_df_optimized_with_failure(mock_plan_df_optimized: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame with optimizations confirmed and a failure."""
    plan_df = mock_plan_df_optimized.copy()
    plan_df.loc[_FAILURE_IDX, IntermediateColumns.OPTIMIZED] = False

    return plan_df


@typechecked
def register_optimizations(
    plan_df: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Register POST requests for optimization launches."""
    responses = {}
    for _, row in plan_df.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        responses[plan_id] = {
            CircuitColumns.ID: plan_id.replace(
                CircuitColumns.PLANS, CircuitColumns.OPERATIONS
            ),
            CircuitColumns.DONE: False,
            CircuitColumns.METADATA: (
                {CircuitColumns.CANCELED: False}
                if row[IntermediateColumns.OPTIMIZED]
                else {CircuitColumns.CANCELED: True}
            ),
        }

    for plan_id in responses:
        requests_mock.register_uri(
            "POST",
            f"{CIRCUIT_URL}/{plan_id}:optimize",
            json=responses[plan_id],
            status_code=200,
        )


@pytest.fixture
@typechecked
def mock_optimization_launches(
    mock_plan_df_optimized: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for optimization launches."""
    register_optimizations(plan_df=mock_plan_df_optimized, requests_mock=requests_mock)


@pytest.fixture
@typechecked
def mock_optimization_launches_failure(
    mock_plan_df_optimized_with_failure: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for optimization launches."""
    register_optimizations(
        plan_df=mock_plan_df_optimized_with_failure, requests_mock=requests_mock
    )


@pytest.fixture
@typechecked
def mock_optimization_launches_after_failure(
    mock_plan_df_stops_uploaded_with_error: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for optimization launches."""
    plan_df = mock_plan_df_stops_uploaded_with_error.copy()
    plan_df = plan_df[plan_df[IntermediateColumns.STOPS_UPLOADED] == True]  # noqa: E712
    plan_df[IntermediateColumns.OPTIMIZED] = True
    register_optimizations(plan_df=plan_df, requests_mock=requests_mock)


@pytest.fixture
@typechecked
def mock_plan_df_confirmed(mock_plan_df_optimized: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame with optimizations confirmed."""
    return mock_plan_df_optimized.copy()


@pytest.fixture
@typechecked
def mock_plan_df_confirmed_with_failure(mock_plan_df_confirmed: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame with optimizations confirmed."""
    plan_df = mock_plan_df_confirmed.copy()
    plan_df.loc[_FAILURE_IDX, IntermediateColumns.OPTIMIZED] = False

    return plan_df


@typechecked
def register_optimization_confirmations(
    plan_df: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Register GET requests for optimization confirmations."""
    responses = {}
    for _, row in plan_df.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        responses[plan_id] = {
            CircuitColumns.ID: plan_id.replace(
                CircuitColumns.PLANS, CircuitColumns.OPERATIONS
            ),
            CircuitColumns.DONE: True,
            CircuitColumns.METADATA: {
                CircuitColumns.CANCELED: False if row[IntermediateColumns.OPTIMIZED] else True
            },
        }

    for plan_id in responses:
        requests_mock.register_uri(
            "GET",
            (
                f"{CIRCUIT_URL}/"
                f"{plan_id.replace(CircuitColumns.PLANS, CircuitColumns.OPERATIONS)}"
            ),
            json=responses[plan_id],
            status_code=200,
        )


@pytest.fixture
@typechecked
def mock_optimization_confirmations(
    mock_plan_df_confirmed: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.get calls for optimization confirmations."""
    register_optimization_confirmations(
        plan_df=mock_plan_df_confirmed, requests_mock=requests_mock
    )


@pytest.fixture
@typechecked
def mock_optimization_confirmations_failure(
    mock_plan_df_confirmed_with_failure: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.get calls for optimization confirmations."""
    register_optimization_confirmations(
        plan_df=mock_plan_df_confirmed_with_failure, requests_mock=requests_mock
    )


@pytest.fixture
@typechecked
def mock_optimization_confirmations_after_failure(
    mock_plan_df_confirmed_with_failure: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.get calls for optimization confirmations."""
    plan_df = mock_plan_df_confirmed_with_failure.copy()
    plan_df = plan_df[plan_df[IntermediateColumns.OPTIMIZED] == True]  # noqa: E712
    register_optimization_confirmations(plan_df=plan_df, requests_mock=requests_mock)


@pytest.fixture
@typechecked
def mock_plan_df_distributed(mock_plan_df_confirmed: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame with optimizations confirmed."""
    plan_df = mock_plan_df_confirmed.copy()
    plan_df[CircuitColumns.DISTRIBUTED] = True

    return plan_df


@pytest.fixture
@typechecked
def mock_route_distributions(
    mock_plan_df_distributed: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for route distributions."""
    responses = {}
    for _, row in mock_plan_df_distributed.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        responses[plan_id] = {CircuitColumns.DISTRIBUTED: True}

    for plan_id in responses:
        requests_mock.register_uri(
            "POST",
            f"{CIRCUIT_URL}/{plan_id}:distribute",
            json=responses[plan_id],
            status_code=200,
        )


# TODO: Abstract what is common between this and the success case.
@pytest.fixture
@typechecked
def mock_route_distributions_failure(
    mock_plan_df_distributed: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for route distributions."""
    responses = {}
    for idx, row in mock_plan_df_distributed.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        responses[plan_id] = {
            CircuitColumns.DISTRIBUTED: True if idx != _FAILURE_IDX else False
        }

    for plan_id in responses:
        requests_mock.register_uri(
            "POST",
            f"{CIRCUIT_URL}/{plan_id}:distribute",
            json=responses[plan_id],
            status_code=200,
        )


@pytest.fixture
@typechecked
def mock_route_distributions_after_failure(
    mock_plan_df_distributed: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for route distributions."""
    responses = {}
    for idx, row in mock_plan_df_distributed.iterrows():
        if idx != _FAILURE_IDX:
            plan_id = row[IntermediateColumns.PLAN_ID]
            responses[plan_id] = {CircuitColumns.DISTRIBUTED: True}

    for plan_id in responses:
        requests_mock.register_uri(
            "POST",
            f"{CIRCUIT_URL}/{plan_id}:distribute",
            json=responses[plan_id],
            status_code=200,
        )


@typechecked
def test_get_all_drivers(mock_driver_df: pd.DataFrame, mock_get_all_drivers: None) -> None:
    """Test that _get_all_drivers retrieves all drivers from the Circuit API."""
    drivers_df = _get_all_drivers()

    pd.testing.assert_frame_equal(
        drivers_df.sort_values(by=drivers_df.columns.tolist()).reset_index(drop=True),
        mock_driver_df.sort_values(by=drivers_df.columns.tolist()).reset_index(drop=True),
    )

    pd.testing.assert_frame_equal(
        drivers_df, drivers_df.sort_values(by=CircuitColumns.NAME).reset_index(drop=True)
    )


@pytest.mark.parametrize(
    "assignment_fixture",
    [
        "mock_driver_assignment",
        "mock_driver_assignment_with_derps",
        "mock_driver_assignment_with_retry",
    ],
)
@typechecked
def test_assign_drivers(
    mock_plan_df_initial: pd.DataFrame,
    mock_driver_df: pd.DataFrame,
    mock_plan_df_drivers_assigned: pd.DataFrame,
    assignment_fixture: str,
    request: pytest.FixtureRequest,
) -> None:
    """Test that _assign_drivers assigns drivers to routes correctly."""
    _ = request.getfixturevalue(assignment_fixture)

    result_df = _assign_drivers(drivers_df=mock_driver_df, plan_df=mock_plan_df_initial)

    result_df[CircuitColumns.ACTIVE] = result_df[CircuitColumns.ACTIVE].astype(bool)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_drivers_assigned)


@pytest.mark.parametrize(
    "assignment_fixture, get_drivers_fixture, error_context",
    [
        ("mock_driver_assignment", "mock_get_all_drivers", nullcontext()),
        ("mock_driver_assignment_with_derps", "mock_get_all_drivers", nullcontext()),
        ("mock_driver_assignment_with_retry", "mock_get_all_drivers", nullcontext()),
        (
            "mock_driver_assignment",
            "mock_get_all_drivers_with_inactive",
            pytest.raises(
                ValueError, match="Inactive drivers. Please activate the following drivers"
            ),
        ),
    ],
)
@typechecked
def test_assign_drivers_to_plans(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_drivers_assigned: pd.DataFrame,
    assignment_fixture: str,
    get_drivers_fixture: str,
    error_context: AbstractContextManager,
    request: pytest.FixtureRequest,
) -> None:
    """Test that _assign_drivers_to_plans assigns drivers to routes correctly."""
    _ = request.getfixturevalue(get_drivers_fixture)
    _ = request.getfixturevalue(assignment_fixture)

    with error_context:
        result_df = _assign_drivers_to_plans(stops_df=mock_stops_df)

        result_df[CircuitColumns.ACTIVE] = result_df[CircuitColumns.ACTIVE].astype(bool)
        pd.testing.assert_frame_equal(result_df, mock_plan_df_drivers_assigned)


@pytest.mark.parametrize(
    "initialization_fixture, plans_df_fixture",
    [
        ("mock_plan_initialization", "mock_plan_df_plans_initialized"),
        ("mock_plan_initialization_failure", "mock_plan_df_plans_initialized_with_failure"),
    ],
)
@typechecked
def test_initialize_plans(
    mock_plan_df_drivers_assigned: pd.DataFrame,
    initialization_fixture: str,
    plans_df_fixture: str,
    request: pytest.FixtureRequest,
) -> None:
    """Test that _initialize_plans initializes plans correctly."""
    _ = request.getfixturevalue(initialization_fixture)
    expected_plan_df = request.getfixturevalue(plans_df_fixture)

    plan_df = _initialize_plans(
        plan_df=mock_plan_df_drivers_assigned, start_date=_START_DATE, verbose=False
    )

    pd.testing.assert_frame_equal(plan_df, expected_plan_df)


# TODO: Check request histories.
@pytest.mark.parametrize(
    "assignment_fixture, get_drivers_fixture, initialization_fixture, error_context",
    [
        (
            "mock_driver_assignment",
            "mock_get_all_drivers",
            "mock_plan_initialization",
            nullcontext(),
        ),
        (
            "mock_driver_assignment_with_derps",
            "mock_get_all_drivers",
            "mock_plan_initialization",
            nullcontext(),
        ),
        (
            "mock_driver_assignment_with_retry",
            "mock_get_all_drivers",
            "mock_plan_initialization",
            nullcontext(),
        ),
        (
            "mock_driver_assignment",
            "mock_get_all_drivers_with_inactive",
            "mock_plan_initialization",
            pytest.raises(
                ValueError, match="Inactive drivers. Please activate the following drivers"
            ),
        ),
        (
            "mock_driver_assignment",
            "mock_get_all_drivers",
            "mock_plan_initialization_failure",
            nullcontext(),
        ),
    ],
)
@typechecked
def test_create_plans(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_plans_initialized: pd.DataFrame,
    get_drivers_fixture: str,
    assignment_fixture: str,
    initialization_fixture: str,
    error_context: AbstractContextManager,
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    """Test that _create_plans creates plans correctly."""
    _ = request.getfixturevalue(get_drivers_fixture)
    _ = request.getfixturevalue(assignment_fixture)
    _ = request.getfixturevalue(initialization_fixture)
    expected_plan_df = (
        request.getfixturevalue("mock_plan_df_plans_initialized")
        if initialization_fixture == "mock_plan_initialization"
        else request.getfixturevalue("mock_plan_df_plans_initialized_with_failure")
    )
    expected_plan_df[CircuitColumns.ACTIVE] = expected_plan_df[CircuitColumns.ACTIVE].astype(
        object
    )

    with error_context:
        plan_df = _create_plans(
            stops_df=mock_stops_df,
            start_date=_START_DATE,
            plan_df_path=tmp_path / "plan_df.csv",
            verbose=False,
        )

        pd.testing.assert_frame_equal(plan_df, expected_plan_df)


@typechecked
def test_create_plans_writes_if_initialization_raises(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_plans_initialized: pd.DataFrame,
    mock_get_all_drivers: None,
    mock_driver_assignment: None,
    mock_plan_initialization: None,
    tmp_path: Path,
) -> None:
    """Test that _create_plans writes the plan_df if initialization raises."""
    plan_df_path = tmp_path / "plan_df.csv"
    with patch(
        "bfb_delivery.lib.dispatch.write_to_circuit._initialize_plans"
    ) as mock_initalize:
        mock_initalize.side_effect = ValueError("Something went wrong.")

        with pytest.raises(ValueError, match="Something went wrong"):
            _ = _create_plans(
                stops_df=mock_stops_df,
                start_date=_START_DATE,
                plan_df_path=plan_df_path,
                verbose=False,
            )

    assert plan_df_path.exists()


# TODO: Expanded test data so we have more stops per route.
# TODO: Test errors etc.:
# - Marks failed as False.
@typechecked
def test_upload_stops_calls(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_plans_initialized: pd.DataFrame,
    mock_stop_upload: dict[str, list],
    requests_mock: Mocker,  # noqa: F811
) -> None:
    """Test that _upload_stops uploads stops correctly."""
    _ = _upload_stops(
        stops_df=mock_stops_df, plan_df=mock_plan_df_plans_initialized, verbose=False
    )
    for plan_id, expected_stop_array in mock_stop_upload.items():
        expected_url = f"{CIRCUIT_URL}/{plan_id}/stops:import"
        matching_requests = [
            req for req in requests_mock.request_history if req.url == expected_url
        ]
        assert len(matching_requests) == 1

        actual_payload = matching_requests[0].json()
        assert actual_payload == expected_stop_array


# @pytest.mark.parametrize(
#     ""
# )
@typechecked
def test_upload_stops_return(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_plans_initialized: pd.DataFrame,
    mock_plan_df_stops_uploaded: pd.DataFrame,
    mock_stop_upload: dict[str, list],
) -> None:
    """Test that _upload_stops uploads stops correctly."""
    result_df = _upload_stops(
        stops_df=mock_stops_df, plan_df=mock_plan_df_plans_initialized, verbose=False
    )
    pd.testing.assert_frame_equal(result_df, mock_plan_df_stops_uploaded)


# TODO: Test errors etc.:
# - Marks failed as False.
# TODO: Test _confirm_optimizations.
@typechecked
def test_optimize_routes(
    mock_plan_df_stops_uploaded: pd.DataFrame,
    mock_plan_df_confirmed: pd.DataFrame,
    mock_optimization_launches: None,
    mock_optimization_confirmations: None,
) -> None:
    """Test that _optimize_routes optimizes routes correctly."""
    result_df = _optimize_routes(plan_df=mock_plan_df_stops_uploaded, verbose=False)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_confirmed)


# TODO: Test errors etc.:
# - Marks failed as False.
@typechecked
def test_distribute_routes(
    mock_plan_df_confirmed: pd.DataFrame,
    mock_plan_df_distributed: pd.DataFrame,
    mock_route_distributions: None,
) -> None:
    """Test that _distribute_routes distributes routes correctly."""
    result_df = _distribute_routes(plan_df=mock_plan_df_confirmed, verbose=False)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_distributed)


@pytest.mark.parametrize("no_distribute", [True, False])
@pytest.mark.parametrize(
    [
        "mock_get_all_drivers_name",
        "mock_driver_assignment_name",
        "mock_plan_initialization_name",
        "mock_stop_upload_name",
        "mock_optimization_launches_name",
        "mock_optimization_confirmations_name",
        "mock_route_distributions_name",
    ],
    [
        (  # No failure.
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches",
            "mock_optimization_confirmations",
            "mock_route_distributions",
        ),
        (  # Failure at plan initialization.
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization_failure",
            "mock_stop_upload_after_failure",
            "mock_optimization_launches_after_failure",
            "mock_optimization_confirmations_after_failure",
            "mock_route_distributions_after_failure",
        ),
        (  # Failure at stop upload.
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload_failure",
            "mock_optimization_launches_after_failure",
            "mock_optimization_confirmations_after_failure",
            "mock_route_distributions_after_failure",
        ),
        (  # Failure at optimization launch.
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches_failure",
            "mock_optimization_confirmations_after_failure",
            "mock_route_distributions_after_failure",
        ),
        (  # Failure at optimization confirmation.
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches",
            "mock_optimization_confirmations_failure",
            "mock_route_distributions_after_failure",
        ),
        (  # Failure at route distribution.
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches",
            "mock_optimization_confirmations",
            "mock_route_distributions_failure",
        ),
    ],
)
@typechecked
def test_upload_split_chunked_calls(
    no_distribute: bool,
    mock_split_chunked_sheet: Path,
    mock_get_all_drivers_name: str,
    mock_driver_assignment_name: str,
    mock_plan_initialization_name: str,
    mock_stop_upload_name: str,
    mock_optimization_launches_name: str,
    mock_optimization_confirmations_name: str,
    mock_route_distributions_name: str,
    requests_mock: Mocker,  # noqa: F811
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    """Test that upload_split_chunked builds, optimizes, and distributes routes correctly."""
    _ = request.getfixturevalue(mock_get_all_drivers_name)
    _ = request.getfixturevalue(mock_driver_assignment_name)
    _ = request.getfixturevalue(mock_plan_initialization_name)
    mock_stop_upload: dict[str, list] = request.getfixturevalue(mock_stop_upload_name)
    _ = request.getfixturevalue(mock_optimization_launches_name)
    _ = request.getfixturevalue(mock_optimization_confirmations_name)
    _ = request.getfixturevalue(mock_route_distributions_name)

    _ = upload_split_chunked(
        split_chunked_workbook_fp=mock_split_chunked_sheet,
        output_dir=tmp_path,
        start_date=_START_DATE,
        no_distribute=no_distribute,
        verbose=False,
    )

    for plan_id, expected_stop_array in mock_stop_upload.items():
        expected_url = f"{CIRCUIT_URL}/{plan_id}/stops:import"
        matching_requests = [
            req for req in requests_mock.request_history if req.url == expected_url
        ]
        assert len(matching_requests) == 1

        actual_payload = matching_requests[0].json()
        assert actual_payload == expected_stop_array


@pytest.mark.parametrize("no_distribute", [True, False])
@pytest.mark.parametrize(
    [
        "failure_step",
        "mock_get_all_drivers_name",
        "mock_driver_assignment_name",
        "mock_plan_initialization_name",
        "mock_stop_upload_name",
        "mock_optimization_launches_name",
        "mock_optimization_confirmations_name",
        "mock_route_distributions_name",
    ],
    [
        (
            "no_failure",
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches",
            "mock_optimization_confirmations",
            "mock_route_distributions",
        ),
        (
            "initialize_plans",
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization_failure",
            "mock_stop_upload_after_failure",
            "mock_optimization_launches_after_failure",
            "mock_optimization_confirmations_after_failure",
            "mock_route_distributions_after_failure",
        ),
        (
            "upload_stops",
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload_failure",
            "mock_optimization_launches_after_failure",
            "mock_optimization_confirmations_after_failure",
            "mock_route_distributions_after_failure",
        ),
        (
            "launch_optimizations",
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches_failure",
            "mock_optimization_confirmations_after_failure",
            "mock_route_distributions_after_failure",
        ),
        (
            "confirm_optimizations",
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches",
            "mock_optimization_confirmations_failure",
            "mock_route_distributions_after_failure",
        ),
        (
            "distribute_routes",
            "mock_get_all_drivers",
            "mock_driver_assignment",
            "mock_plan_initialization",
            "mock_stop_upload",
            "mock_optimization_launches",
            "mock_optimization_confirmations",
            "mock_route_distributions_failure",
        ),
    ],
)
@typechecked
def test_upload_split_chunked_return(
    no_distribute: bool,
    mock_split_chunked_sheet: Path,
    mock_plan_df_distributed: pd.DataFrame,
    failure_step: str,
    mock_get_all_drivers_name: str,
    mock_driver_assignment_name: str,
    mock_plan_initialization_name: str,
    mock_stop_upload_name: str,
    mock_optimization_launches_name: str,
    mock_optimization_confirmations_name: str,
    mock_route_distributions_name: str,
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    """Test that upload_split_chunked builds, optimizes, and distributes routes correctly."""
    _ = request.getfixturevalue(mock_get_all_drivers_name)
    _ = request.getfixturevalue(mock_driver_assignment_name)
    _ = request.getfixturevalue(mock_plan_initialization_name)
    _ = request.getfixturevalue(mock_stop_upload_name)
    _ = request.getfixturevalue(mock_optimization_launches_name)
    _ = request.getfixturevalue(mock_optimization_confirmations_name)
    _ = request.getfixturevalue(mock_route_distributions_name)

    result_df = upload_split_chunked(
        split_chunked_workbook_fp=mock_split_chunked_sheet,
        output_dir=tmp_path,
        start_date=_START_DATE,
        no_distribute=no_distribute,
        verbose=False,
    )

    expected_df = mock_plan_df_distributed.copy()
    expected_df[IntermediateColumns.START_DATE] = _START_DATE
    expected_df[CircuitColumns.ACTIVE] = expected_df[CircuitColumns.ACTIVE].astype(object)

    failure_route_title = expected_df[IntermediateColumns.ROUTE_TITLE].iloc[_FAILURE_IDX]

    if failure_step == "initialize_plans":
        expected_df.loc[
            expected_df[IntermediateColumns.ROUTE_TITLE] == failure_route_title,
            IntermediateColumns.INITIALIZED,
        ] = False
        expected_df.loc[
            expected_df[IntermediateColumns.ROUTE_TITLE] == failure_route_title,
            CircuitColumns.WRITABLE,
        ] = False
    if failure_step in ["initialize_plans", "upload_stops"]:
        expected_df.loc[
            expected_df[IntermediateColumns.ROUTE_TITLE] == failure_route_title,
            IntermediateColumns.STOPS_UPLOADED,
        ] = False
    if failure_step in ["initialize_plans", "upload_stops", "launch_optimizations"]:
        expected_df.loc[
            expected_df[IntermediateColumns.ROUTE_TITLE] == failure_route_title,
            IntermediateColumns.OPTIMIZED,
        ] = False
    if failure_step == "confirm_optimizations":
        expected_df.loc[
            expected_df[IntermediateColumns.ROUTE_TITLE] == failure_route_title,
            IntermediateColumns.OPTIMIZED,
        ] = np.nan
    if failure_step in [
        "initialize_plans",
        "upload_stops",
        "launch_optimizations",
        "confirm_optimizations",
        "distribute_routes",
    ]:
        expected_df.loc[
            expected_df[IntermediateColumns.ROUTE_TITLE] == failure_route_title,
            CircuitColumns.DISTRIBUTED,
        ] = False

    if no_distribute:
        expected_df[CircuitColumns.DISTRIBUTED] = False
        pd.testing.assert_frame_equal(result_df, expected_df)
    else:
        pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "use_plan_ids, plan_df_fp, fail_deletion, error_context",
    [
        (True, "", False, nullcontext()),
        (False, "plans.csv", False, nullcontext()),
        (
            True,
            "plans.csv",
            False,
            pytest.raises(
                ValueError, match="Please provide either plan_ids or plan_ids_fp, not both."
            ),
        ),
        (
            False,
            "",
            False,
            pytest.raises(ValueError, match="Please provide either plan_ids or plan_ids_fp."),
        ),
        (
            True,
            "",
            True,
            pytest.raises(RuntimeError, match="Errors deleting plans:\n{'plans/plan1'"),
        ),
    ],
)
def test_delete_plans(
    use_plan_ids: bool,
    plan_df_fp: str,
    fail_deletion: bool,
    error_context: AbstractContextManager,
    requests_mock: Mocker,  # noqa: F811
    tmp_path: Path,
) -> None:
    """Test that delete_plans deletes plans correctly."""
    plan_ids_to_delete = (
        _DELETE_PLAN_IDS
        if use_plan_ids
        else _DELETE_PLAN_DF[IntermediateColumns.PLAN_ID].tolist()
    )
    for idx, plan_id in enumerate(plan_ids_to_delete):
        requests_mock.register_uri(
            "DELETE",
            f"{CIRCUIT_URL}/{plan_id}",
            json={},
            status_code=(
                204 if not fail_deletion or (fail_deletion and idx != _FAILURE_IDX) else 400
            ),
        )

    input_plan_ids = _DELETE_PLAN_IDS if use_plan_ids else []
    if plan_df_fp:
        plan_df_fp = str(tmp_path / plan_df_fp)
        _DELETE_PLAN_DF.to_csv(plan_df_fp, index=False)

    with error_context:
        returned_plan_ids = delete_plans(plan_ids=input_plan_ids, plan_df_fp=plan_df_fp)

        assert sorted(returned_plan_ids) == sorted(plan_ids_to_delete)


@pytest.mark.parametrize(
    "fail, error_context",
    [
        (False, nullcontext()),
        (True, pytest.raises(ValueError, match="Unexpected response 400")),
    ],
)
@typechecked
def test_delete_plan_call(fail: bool, error_context: AbstractContextManager) -> None:
    """Test that delete_plan deletes a plan correctly."""
    plan_id = "plans/plan1"
    with patch("bfb_delivery.lib.dispatch.api_callers.requests.delete") as mock_delete:
        mock_delete.return_value.status_code = 204 if not fail else 400

        with error_context:
            _ = delete_plan(plan_id=plan_id)

        assert mock_delete.call_args_list[0][1]["url"] == f"{CIRCUIT_URL}/{plan_id}"


@pytest.mark.parametrize(
    "fail, error_context",
    [
        (False, nullcontext()),
        (True, pytest.raises(ValueError, match="Unexpected response 400")),
    ],
)
@typechecked
def test_delete_plan_return(fail: bool, error_context: AbstractContextManager) -> None:
    """Test that delete_plan deletes a plan correctly."""
    plan_id = "plans/plan1"
    with patch("bfb_delivery.lib.dispatch.api_callers.requests.delete") as mock_delete:
        mock_delete.return_value.status_code = 204 if not fail else 400

        with error_context:
            deletion = delete_plan(plan_id=plan_id)
            assert deletion == (not fail)
