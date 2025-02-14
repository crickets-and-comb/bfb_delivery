"""Test suite for write_to_circuit.py."""

# NOTE: We're ignoring the top-level `build_routes_from_chunked` since everything it wraps is
# already under test, except the parts defined in the write_to_circuit module itself, i.e.
# `upload_split_chunked`.

# TODO: Call upload_split_chunked, and check outputs, called withs, raises.
# TODO: Use schema in typehints where applicable.
# TODO: Test that plan_df dictates which next steps are taken.
# Failures should be marked as False, and next steps should not be taken for failed plans.

import builtins
from pathlib import Path
from typing import Any, Final

import pandas as pd
import pytest
import requests_mock  # noqa: F401
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
from bfb_delivery.lib.dispatch.write_to_circuit import (  # upload_split_chunked
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
)

_START_DATE: Final[str] = "2025-01-01"


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
) -> None:
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
    # TODO: Build unit test for this function.
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
def mock_plan_inialization_posts(
    mock_plan_df_plans_initialized: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for plan initialization."""
    responses = {}
    for _, row in mock_plan_df_plans_initialized.iterrows():
        title = row[IntermediateColumns.ROUTE_TITLE]
        responses[title] = {
            CircuitColumns.ID: row[IntermediateColumns.PLAN_ID],
            CircuitColumns.WRITABLE: row[CircuitColumns.WRITABLE],
        }

    def post_callback(request: Any, context: Any) -> Any:
        data = request.json()
        plan_title = data.get(CircuitColumns.TITLE)
        return responses[plan_title]

    requests_mock.register_uri(
        "POST", f"{CIRCUIT_URL}/plans", json=post_callback, status_code=200
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
def mock_stop_upload_posts(
    mock_plan_df_plans_initialized: pd.DataFrame,
    mock_stops_df: pd.DataFrame,
    requests_mock: Mocker,  # noqa: F811
) -> dict[str, list]:
    """Mock requests.post calls for stop uploads."""
    stop_arrays = {}
    responses = {}
    for _, row in mock_plan_df_plans_initialized.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        title = row[IntermediateColumns.ROUTE_TITLE]

        stops_df = mock_stops_df[mock_stops_df[IntermediateColumns.SHEET_NAME] == title]
        # TODO: Build unit test for _parse_addresses.
        stops_df = _parse_addresses(stops_df=stops_df)

        # TODO: Build unit test for _build_stop_array.
        stop_arrays[plan_id] = _build_stop_array(
            route_stops=stops_df, driver_id=row[CircuitColumns.ID]
        )

        responses[plan_id] = {
            "success": [
                "stops/" + row[Columns.NAME] + row[Columns.PRODUCT_TYPE]
                for _, row in stops_df.iterrows()
            ],
            "failed": [],
        }

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
def mock_optmization_launches(
    mock_plan_df_optimized: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.post calls for optimization launches."""
    responses = {}
    for _, row in mock_plan_df_optimized.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        responses[plan_id] = {
            CircuitColumns.ID: plan_id.replace(
                CircuitColumns.PLANS, CircuitColumns.OPERATIONS
            ),
            CircuitColumns.DONE: False,
            CircuitColumns.METADATA: {CircuitColumns.CANCELED: False},
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
def mock_optimization_confirmations(
    mock_plan_df_optimized: pd.DataFrame, requests_mock: Mocker  # noqa: F811
) -> None:
    """Mock requests.get calls for optimization confirmations."""
    responses = {}
    for _, row in mock_plan_df_optimized.iterrows():
        plan_id = row[IntermediateColumns.PLAN_ID]
        responses[plan_id] = {
            CircuitColumns.ID: plan_id.replace(
                CircuitColumns.PLANS, CircuitColumns.OPERATIONS
            ),
            CircuitColumns.DONE: True,
            CircuitColumns.METADATA: {CircuitColumns.CANCELED: False},
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
def mock_plan_df_distributed(mock_plan_df_optimized: pd.DataFrame) -> pd.DataFrame:
    """Return a mock plan DataFrame with optimizations confirmed."""
    plan_df = mock_plan_df_optimized.copy()
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


@typechecked
def test_get_all_drivers(mock_driver_df: pd.DataFrame, mock_all_drivers_simple: None) -> None:
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
@typechecked
def test_assign_drivers(
    mock_plan_df_initial: pd.DataFrame,
    mock_driver_df: pd.DataFrame,
    mock_plan_df_drivers_assigned: pd.DataFrame,
    mock_driver_assignment: None,
) -> None:
    """Test that _assign_drivers assigns drivers to routes correctly."""
    result_df = _assign_drivers(drivers_df=mock_driver_df, plan_df=mock_plan_df_initial)

    result_df[CircuitColumns.ACTIVE] = result_df[CircuitColumns.ACTIVE].astype(bool)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_drivers_assigned)


# TODO: Test errors etc.:
# - Invalid inputs. (Just put them right in the middle of the list?)
# - Retry if confirm.lower() != "y"
# - Raise if inactive drivers selected.
@typechecked
def test_assign_drivers_to_plans(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_drivers_assigned: pd.DataFrame,
    mock_all_drivers_simple: None,
    mock_driver_assignment: None,
) -> None:
    """Test that _assign_drivers_to_plans assigns drivers to routes correctly."""
    result_df = _assign_drivers_to_plans(stops_df=mock_stops_df)

    result_df[CircuitColumns.ACTIVE] = result_df[CircuitColumns.ACTIVE].astype(bool)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_drivers_assigned)


# TODO: Test errors etc.:
# - Marks failed initializations as False.
@typechecked
def test_initialize_plans(
    mock_plan_df_drivers_assigned: pd.DataFrame,
    mock_plan_df_plans_initialized: pd.DataFrame,
    mock_plan_inialization_posts: None,
) -> None:
    """Test that _initialize_plans initializes plans correctly."""
    result_df = _initialize_plans(
        plan_df=mock_plan_df_drivers_assigned, start_date=_START_DATE, verbose=False
    )
    pd.testing.assert_frame_equal(result_df, mock_plan_df_plans_initialized)


# TODO: Test errors etc.:
# - Invalid inputs. (Just put them right in the middle of the list?)
# - Retry if confirm.lower() != "y"
# - Raise if inactive drivers selected.
# - Marks failed initializations as False.
# - Writes plan_df if _initialize_plans raises.
@typechecked
def test_create_plans(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_plans_initialized: pd.DataFrame,
    mock_all_drivers_simple: None,
    mock_driver_assignment: None,
    mock_plan_inialization_posts: None,
    tmp_path: Path,
) -> None:
    """Test that _create_plans creates plans correctly."""
    result_df = _create_plans(
        stops_df=mock_stops_df, start_date=_START_DATE, plan_df_path=tmp_path, verbose=False
    )
    mock_plan_df_plans_initialized[CircuitColumns.ACTIVE] = mock_plan_df_plans_initialized[
        CircuitColumns.ACTIVE
    ].astype(object)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_plans_initialized)


# TODO: Expanded test data so we have more stops per route.
# TODO: Test errors etc.:
# - Marks failed as False.
@typechecked
def test_upload_stops(
    mock_stops_df: pd.DataFrame,
    mock_plan_df_plans_initialized: pd.DataFrame,
    mock_plan_df_stops_uploaded: pd.DataFrame,
    mock_stop_upload_posts: dict[str, list],
    requests_mock: Mocker,  # noqa: F811
) -> None:
    """Test that _upload_stops uploads stops correctly."""
    result_df = _upload_stops(
        stops_df=mock_stops_df, plan_df=mock_plan_df_plans_initialized, verbose=False
    )
    for plan_id, expected_stop_array in mock_stop_upload_posts.items():
        expected_url = f"{CIRCUIT_URL}/{plan_id}/stops:import"
        matching_requests = [
            req for req in requests_mock.request_history if req.url == expected_url
        ]
        assert len(matching_requests) == 1

        actual_payload = matching_requests[0].json()
        assert actual_payload == expected_stop_array

    pd.testing.assert_frame_equal(result_df, mock_plan_df_stops_uploaded)


# TODO: Test errors etc.:
# - Marks failed as False.
# TODO: Test _confirm_optimizations.
@typechecked
def test_optimize_routes(
    mock_plan_df_stops_uploaded: pd.DataFrame,
    mock_plan_df_optimized: pd.DataFrame,
    mock_optmization_launches: None,
    mock_optimization_confirmations: None,
) -> None:
    """Test that _optimize_routes optimizes routes correctly."""
    result_df = _optimize_routes(plan_df=mock_plan_df_stops_uploaded, verbose=False)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_optimized)


# TODO: Test errors etc.:
# - Marks failed as False.
@typechecked
def test_distribute_routes(
    mock_plan_df_optimized: pd.DataFrame,
    mock_plan_df_distributed: pd.DataFrame,
    mock_route_distributions: None,
) -> None:
    """Test that _distribute_routes distributes routes correctly."""
    result_df = _distribute_routes(plan_df=mock_plan_df_optimized, verbose=False)
    pd.testing.assert_frame_equal(result_df, mock_plan_df_distributed)


# # TODO: Test errors etc.:
# # - Marks failed as False at each step amd does not take next steps on failed.
# @pytest.mark.parametrize(
#     "no_distribute", [True, False]
# )
# @typechecked
# def test_upload_split_chunked(
#     no_distribute: bool,
#     mock_split_chunked_sheet: Path,
#     mock_plan_df_optimized: pd.DataFrame,
#     mock_plan_df_distributed: pd.DataFrame,
#     mock_route_distributions: None,
#     tmp_path: Path,
# ) -> None:
#     """Test that upload_split_chunked builds, optimizes, and distributes routes correctly.""" # noqa: E501
#     result_df = upload_split_chunked(
#         split_chunked_workbook_fp=mock_split_chunked_sheet,
#         output_dir = tmp_path,
#         start_date=_START_DATE,
#         no_distribute=no_distribute,
#         verbose=False
#     )
#     if no_distribute:
#         pd.testing.assert_frame_equal(result_df, mock_plan_df_optimized)
#     else:
#         pd.testing.assert_frame_equal(result_df, mock_plan_df_distributed)
