"""Write routes to Circuit."""

import logging
from time import sleep
from typing import Any

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from bfb_delivery.lib.constants import CircuitColumns, Columns, RateLimits
from bfb_delivery.lib.dispatch.api_callers import CheckOptimization, LaunchOptimization

# TODO: Move _concat_response_pages to utils.
from bfb_delivery.lib.dispatch.read_circuit import _concat_response_pages
from bfb_delivery.lib.dispatch.utils import get_circuit_key, get_response_dict, get_responses

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: Just wrap the CLI with split_chunked_routes now from the start.
# TODO: Default to soones Friday.
@typechecked
def upload_split_chunked(
    split_chunked_workbook_fp: str, start_date: str, distribute: bool, verbose: bool
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """Upload a split chunked Excel workbook of routes to circuit.

    The workbook contains multiple sheets, one per route. Each sheet is named after the driver
    with the date. The columns are:
    - Name
    - Address
    - Phone
    - Email
    - Notes
    - Order Count
    - Product Type
    - Neighborhood


    Args:
        split_chunked_workbook_fp: The file path to the split chunked workbook.
        start_date: The date to start the routes, as "YYYY-MM-DD".
        distribute: Whether to distribute the routes after optimizing.
        verbose: Whether to print verbose output.

    Returns:
        A DataFrame with the plan IDs and driver IDs for each sheet,
            along with date and whether distributed.
        A dictionary with the uploaded stops for each plan.
    """
    with pd.ExcelFile(split_chunked_workbook_fp) as workbook:
        stops_dfs = []
        for sheet in workbook.sheet_names:
            df = workbook.parse(sheet)
            df["sheet_name"] = str(sheet)
            stops_dfs.append(df)
        stops_df = pd.concat(stops_dfs).reset_index(drop=True)

        sheet_plan_df = _create_plans(
            stops_df=stops_df, start_date=start_date, verbose=verbose
        )
        uploaded_stops = _upload_stops(
            stops_df=stops_df, sheet_plan_df=sheet_plan_df, verbose=verbose
        )
        _optimize_routes(sheet_plan_df=sheet_plan_df, verbose=verbose)

        if distribute:
            _distribute_routes(sheet_plan_df=sheet_plan_df)

        sheet_plan_df["distributed"] = distribute
        sheet_plan_df["start_date"] = start_date

    return sheet_plan_df, uploaded_stops


@typechecked
def _create_plans(stops_df: pd.DataFrame, start_date: str, verbose: bool) -> pd.DataFrame:
    """Create a plan for each route.

    Args:
        stops_df: The long DataFrame with all the routes.
        start_date: The date to start the routes, as "YYYY-MM-DD".
        verbose: Whether to print verbose output.

    Returns:
        A DataFrame with the plan IDs and driver IDs for each sheet.
    """
    route_driver_df = _get_driver_ids(stops_df=stops_df)
    sheet_plan_df = _initialize_plans(
        route_driver_df=route_driver_df, start_date=start_date, verbose=verbose
    )

    return sheet_plan_df


@typechecked
def _upload_stops(
    stops_df: pd.DataFrame, sheet_plan_df: pd.DataFrame, verbose: bool
) -> dict[str, list[str]]:
    """Upload stops for each route.

    Args:
        stops_df: The long DataFrame with all the routes.
        sheet_plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.
        verbose: Whether to print verbose output.

    Returns:
        A dictionary with the uploaded stops for each plan.
    """
    plan_stops = _build_plan_stops(stops_df=stops_df, sheet_plan_df=sheet_plan_df)

    logger.info("Uploading stops ...")
    uploaded_stops = {}
    stop_id_count = 0
    for plan_id, stop_arrays in plan_stops.items():
        if verbose:
            route_title = sheet_plan_df[sheet_plan_df["plan_id"] == plan_id][
                "route_title"
            ].values[0]
            logger.info(f"Uploading stops for {route_title} ({plan_id}) ...")
        for stop_array in stop_arrays:
            stop_ids, _ = _upload_stop_array(
                plan_id=plan_id, stop_array=stop_array, wait_seconds=RateLimits.WRITE_SECONDS
            )
            uploaded_stops[plan_id] = stop_ids
            stop_id_count += len(stop_ids)

    logger.info(
        f"Finished uploading stops. Uploaded {stop_id_count} stops for "
        f"{len(uploaded_stops)} plans."
    )

    return uploaded_stops


@typechecked
def _optimize_routes(sheet_plan_df: pd.DataFrame, verbose: bool) -> None:
    """Optimize the routes."""
    logger.info("Initializing route optimizations ...")
    plan_ids = sheet_plan_df["plan_id"].to_list()
    optimizations = {}

    errors = []
    for plan_id in plan_ids:
        plan_title = sheet_plan_df[sheet_plan_df["plan_id"] == plan_id]["route_title"].values[
            0
        ]
        if verbose:
            logger.info(f"Optimizing route for {plan_title} ({plan_id}) ...")
        # TODO: If optimization comes back finished on launch, don't check later.
        optimization = LaunchOptimization(plan_id=plan_id, plan_title=plan_title)
        try:
            optimization.call_api()
        except Exception as e:
            logger.error(
                f"Error launching optimization for {plan_title} ({plan_id}):"
                f"\n{optimization._response_json}"
            )
            errors.append(e)
        else:
            optimizations[plan_id] = optimization.operation_id
            if verbose:
                logger.info(
                    f"Launched optimization for {plan_title} ({plan_id}): "
                    f"{optimization.operation_id}"
                )

    if errors:
        raise RuntimeError(f"Errors launching optimizations:\n{errors}")

    _wait_for_optimizations(
        sheet_plan_df=sheet_plan_df, optimizations=optimizations, verbose=verbose
    )


# TODO: Make a CLI for this since it will be optional.
@typechecked
def _distribute_routes(sheet_plan_df: pd.DataFrame) -> None:
    """Distribute the routes."""
    for plan_id in sheet_plan_df["plan_id"].to_list():
        _distribute_route(plan_id=plan_id)


@typechecked
def _get_driver_ids(stops_df: pd.DataFrame) -> pd.DataFrame:
    """Get the driver IDs for each sheet.

    Args:
        stops_df: The long DataFrame with all the routes.

    Returns:
        A DataFrame with the driver IDs for each sheet.
    """
    route_driver_df = pd.DataFrame(
        {
            "route_title": stops_df["sheet_name"].unique(),
            "driver_name": None,
            "email": None,
            "id": None,
        }
    )

    drivers_df = _get_all_drivers()
    route_driver_df = _assign_drivers(drivers_df=drivers_df, route_driver_df=route_driver_df)
    inactive_drivers = route_driver_df[~(route_driver_df["active"])]["driver_name"].tolist()
    if inactive_drivers:
        raise ValueError(
            (
                "Inactive drivers. Please activate the following drivers before creating "
                f"routes for them: {inactive_drivers}"
            )
        )

    return route_driver_df


@typechecked
def _initialize_plans(
    route_driver_df: pd.DataFrame, start_date: str, verbose: bool
) -> pd.DataFrame:
    """Initialize plans for each driver.

    Args:
        route_driver_df: The DataFrame with the driver IDs for each sheet.
        start_date: The date to start the routes, as "YYYY-MM-DD".

    Returns:
        A DataFrame with the plan IDs and driver IDs for each sheet.
    """
    route_driver_df["plan_id"] = None
    route_driver_df["depot"] = None
    route_driver_df["writeable"] = None
    route_driver_df["optimization"] = None

    logger.info("Initializing plans ...")
    wait_seconds = RateLimits.WRITE_SECONDS
    try:
        for idx, row in route_driver_df.iterrows():
            plan_data = {
                "title": row["route_title"],
                "starts": {
                    "day": int(start_date.split("-")[2]),
                    "month": int(start_date.split("-")[1]),
                    "year": int(start_date.split("-")[0]),
                },
                "drivers": [row["id"]],
            }
            if verbose:
                logger.info(f"Creating plan for {row['route_title']} ...")
            plan_response, wait_seconds = _post_plan(
                plan_data=plan_data, wait_seconds=wait_seconds
            )

            if verbose:
                logger.info(
                    f"Created plan {plan_response['id']} for {row['route_title']}."
                    f"\n{plan_response}"
                )

            route_driver_df.loc[idx, ["plan_id", "writeable"]] = (
                plan_response["id"],
                plan_response["writable"],
            )

    except Exception as e:
        # Can't clean up plans if they're not finished.
        logger.error(f"Error initializing plans:\n{route_driver_df}")
        raise e

    not_writable = route_driver_df[route_driver_df["writeable"] == False]  # noqa: E712
    if not not_writable.empty:
        raise ValueError(f"Plan is not writable for the following routes:\n{not_writable}")

    logger.info(f"Finished initializing plans. Initialized {idx + 1} plans.")

    return route_driver_df


@typechecked
def _build_plan_stops(
    stops_df: pd.DataFrame, sheet_plan_df: pd.DataFrame
) -> dict[str, list[list[dict[str, dict[str, str] | list[str] | int | str]]]]:
    """Build stop arrays for each route.

    Args:
        stops_df: The long DataFrame with all the routes.
        sheet_plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.

    Returns:
        For each plan, a list of stop arrays for batch stop uploads.
    """
    stops_df = _parse_addresses(stops_df=stops_df)
    plan_stops = {}
    for _, plan_row in sheet_plan_df.iterrows():
        plan_id = plan_row["plan_id"]
        route_title = plan_row["route_title"]
        route_stops = stops_df[stops_df["sheet_name"] == route_title]
        plan_stops[plan_id] = _build_stop_array(
            route_stops=route_stops, driver_id=plan_row["id"]
        )

    for plan_id, all_stops in plan_stops.items():
        stop_arrays = []
        # Split all_stops into chunks of 100 stops.
        number_of_stops = len(all_stops)
        for i in range(0, number_of_stops, 100):
            stop_arrays.append(
                all_stops[i : i + 100]  # noqa: E203
            )  # TODO: Add noqa E203 to shared, and remove throughout codebase.
        plan_stops[plan_id] = stop_arrays

    return plan_stops


@typechecked
def _distribute_route(plan_id: str) -> None:
    """Distribute a route."""
    pass


@typechecked
def _get_all_drivers() -> pd.DataFrame:
    """Get all drivers."""
    url = "https://api.getcircuit.com/public/v0.2b/drivers"
    logger.info("Getting all drivers from Circuit ...")
    driver_pages = get_responses(url=url)
    logger.info("Finished getting drivers.")
    drivers_list = _concat_response_pages(page_list=driver_pages, data_key="drivers")
    drivers_df = pd.DataFrame(drivers_list)
    drivers_df = drivers_df.sort_values(by="name").reset_index(drop=True)

    return drivers_df


@typechecked
def _assign_drivers(drivers_df: pd.DataFrame, route_driver_df: pd.DataFrame) -> pd.DataFrame:
    """Ask users to assign drivers to each route."""
    for idx, row in drivers_df.iterrows():
        print(f"{idx + 1}. {row['name']} {row['email']} ({row['id']})")

    print("\nUsing the driver numbers above, assign drivers to each route:")
    for route_title in route_driver_df["route_title"]:
        route_driver_df = _assign_driver(
            route_title=route_title, drivers_df=drivers_df, route_driver_df=route_driver_df
        )

    for _, row in route_driver_df.iterrows():
        print(f"{row['route_title']}: " f"{row['driver_name']}, {row['email']} ({row['id']})")
    confirm = input("Confirm the drivers above? (y/n): ")
    if confirm.lower() != "y":
        route_driver_df = _assign_drivers(
            drivers_df=drivers_df, route_driver_df=route_driver_df
        )

    return route_driver_df


@typechecked
def _post_plan(plan_data: dict, wait_seconds: float) -> tuple[dict, float]:
    """Post a plan to Circuit."""
    response = requests.post(
        url="https://api.getcircuit.com/public/v0.2b/plans",
        json=plan_data,
        auth=HTTPBasicAuth(get_circuit_key(), ""),
        timeout=RateLimits.WRITE_TIMEOUT_SECONDS,
    )

    # TODO: Abstract this try block.
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_e:
        response_dict = get_response_dict(response=response)
        err_msg = (
            f"Got {response.status_code} reponse for {plan_data['title']}: {response_dict}"
        )
        raise requests.exceptions.HTTPError(err_msg) from http_e

    else:
        if response.status_code == 200:
            plan = response.json()
        elif response.status_code == 429:
            wait_seconds = wait_seconds * 2
            logger.warning(f"Rate-limited. Waiting {wait_seconds} seconds to retry.")
            sleep(wait_seconds)
            plan, wait_seconds = _post_plan(plan_data=plan_data, wait_seconds=wait_seconds)
        else:
            response_dict = get_response_dict(response=response)
            raise ValueError(f"Unexpected response {response.status_code}: {response_dict}")

    return plan, wait_seconds


@typechecked
def _assign_driver(
    route_title: str, drivers_df: pd.DataFrame, route_driver_df: pd.DataFrame
) -> pd.DataFrame:
    """Ask user to assign driver to a route."""
    best_guesses = pd.DataFrame()
    for name_part in route_title.split(" ")[1:]:
        if name_part not in ["&", "AND"] and len(name_part) > 1:
            best_guesses = pd.concat(
                [
                    best_guesses,
                    drivers_df[drivers_df["name"].str.contains(name_part, case=False)],
                ]
            )
    best_guesses = best_guesses.drop_duplicates().sort_values(by="name")

    print(f"\nRoute: {route_title}")
    print("Choose any of the possible drivers above, but here are some best guesses:")
    for idx, driver in best_guesses.iterrows():
        print(f"{idx + 1}. {driver['name']} {driver['email']} (ID: {driver['id']})")

    assigned = False
    while not assigned:
        try:
            # TODO: Wrap this for test mock.
            # TODO: Add B907 to shared ignore list, and remove r"" throughout.
            choice = input(
                f"Enter the number of the driver for '{route_title}':"  # noqa: B907
            )

        except ValueError:
            print("Invalid input. Please enter a number.")

        else:
            choice = choice if choice else "-1"
            try:
                choice = int(choice.strip()) - 1
            except ValueError:
                print("Invalid input. Please enter a number.")
            else:
                route_driver_df.loc[
                    route_driver_df["route_title"] == route_title,
                    ["id", "driver_name", "email", "active"],
                ] = [
                    drivers_df.iloc[choice]["id"],
                    drivers_df.iloc[choice]["name"],
                    drivers_df.iloc[choice]["email"],
                    drivers_df.iloc[choice]["active"],
                ]
                assigned = True

    return route_driver_df


@typechecked
def _parse_addresses(stops_df: pd.DataFrame) -> pd.DataFrame:
    """Parse addresses for each route."""
    stops_df[CircuitColumns.ADDRESS_NAME] = stops_df[Columns.ADDRESS]
    stops_df[CircuitColumns.ADDRESS_LINE_1] = ""
    stops_df[CircuitColumns.ADDRESS_LINE_2] = ""
    stops_df[CircuitColumns.STATE] = "WA"
    stops_df[CircuitColumns.ZIP] = ""
    stops_df[CircuitColumns.COUNTRY] = "US"

    for idx, row in stops_df.iterrows():
        address = row[Columns.ADDRESS]
        split_address = address.split(",")
        stops_df.at[idx, CircuitColumns.ADDRESS_LINE_1] = split_address[0].strip()
        stops_df.at[idx, CircuitColumns.ADDRESS_LINE_2] = ", ".join(
            [part.strip() for part in split_address[1:]]
        )
        stops_df.at[idx, CircuitColumns.ZIP] = split_address[-1]

    return stops_df


@typechecked
def _upload_stop_array(
    plan_id: str,
    stop_array: list[dict[str, dict[str, str] | list[str] | int | str]],
    wait_seconds: float,
) -> tuple[list[str], float]:
    """Upload a stop array."""
    response = requests.post(
        url=f"https://api.getcircuit.com/public/v0.2b/{plan_id}/stops:import",
        json=stop_array,
        auth=HTTPBasicAuth(get_circuit_key(), ""),
        timeout=RateLimits.WRITE_TIMEOUT_SECONDS,
    )

    # TODO: Abstract this try block.
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_e:
        response_dict = get_response_dict(response=response)
        err_msg = f"Got {response.status_code} reponse for {plan_id}: {response_dict}"
        raise requests.exceptions.HTTPError(err_msg) from http_e

    else:
        if response.status_code == 200:
            stops = response.json()
            stop_ids = stops["success"]
            failed = stops.get("failed")
            if failed:
                raise RuntimeError(f"Failed to upload stops:\n{failed}")
            elif len(stop_ids) != len(stop_array):
                raise RuntimeError(
                    f"Did not upload same number of stops as input:\n{stop_ids}\n{stop_array}"
                )

        elif response.status_code == 429:
            wait_seconds = wait_seconds * 2
            logger.warning(f"Rate-limited. Waiting {wait_seconds} seconds to retry.")
            sleep(wait_seconds)
            stop_ids, wait_seconds = _upload_stop_array(
                plan_id=plan_id, stop_array=stop_array, wait_seconds=wait_seconds
            )
        else:
            response_dict = get_response_dict(response=response)
            raise ValueError(f"Unexpected response {response.status_code}: {response_dict}")

    return stop_ids, wait_seconds


@typechecked
def _wait_for_optimizations(
    sheet_plan_df: pd.DataFrame, optimizations: dict[str, str], verbose: bool
) -> None:
    """Wait for all optimizations to finish."""
    logger.info("Waiting for optimizations to finish ...")
    optimizations_finished: dict[str, bool | str] = {
        plan_id: False for plan_id in sheet_plan_df["plan_id"].to_list()
    }
    errors = []
    while not all([val is True or val == "error" for val in optimizations_finished.values()]):
        unfinished = [
            plan_id for plan_id, finished in optimizations_finished.items() if not finished
        ]
        for plan_id in unfinished:
            plan_title = sheet_plan_df[sheet_plan_df["plan_id"] == plan_id][
                "route_title"
            ].values[0]
            if verbose:
                logger.info(f"Checking optimization for {plan_title} ({plan_id}) ...")
            check_op = CheckOptimization(
                plan_id=plan_id, operation_id=optimizations[plan_id], plan_title=plan_title
            )
            try:
                check_op.call_api()
            except Exception as e:
                logger.error(
                    f"Error checking optimization for {plan_title} ({plan_id}):"
                    f"\n{check_op._response_json}"
                )
                errors.append(e)
                optimizations_finished[plan_id] = "error"
            else:
                optimizations_finished[plan_id] = check_op.finished
                if verbose:
                    logger.info(
                        f"Optimization status for {plan_title} ({plan_id}): "
                        f"{check_op.finished}"
                    )

    logger.info(
        "Finished optimizing routes. Optimized "
        f"{len([val for val in optimizations_finished.values() if val is True])} routes."
    )

    if errors:
        raise RuntimeError(f"Errors checking optimizations:\n{errors}")


@typechecked
def _build_stop_array(route_stops: pd.DataFrame, driver_id: str) -> list[dict[str, Any]]:
    """Build a stop array for a route."""
    stop_array = []
    for _, stop_row in route_stops.iterrows():
        stop = {
            CircuitColumns.ADDRESS: {
                CircuitColumns.ADDRESS_NAME: stop_row[CircuitColumns.ADDRESS_NAME],
                CircuitColumns.ADDRESS_LINE_1: stop_row[CircuitColumns.ADDRESS_LINE_1],
                CircuitColumns.ADDRESS_LINE_2: stop_row[CircuitColumns.ADDRESS_LINE_2],
                CircuitColumns.STATE: stop_row[CircuitColumns.STATE],
                CircuitColumns.ZIP: stop_row[CircuitColumns.ZIP],
                CircuitColumns.COUNTRY: stop_row[CircuitColumns.COUNTRY],
            },
            CircuitColumns.ORDER_INFO: {
                CircuitColumns.PRODUCTS: [stop_row[Columns.PRODUCT_TYPE]]
            },
            CircuitColumns.ALLOWED_DRIVERS: [driver_id],
            CircuitColumns.PACKAGE_COUNT: stop_row[Columns.ORDER_COUNT],
            CircuitColumns.NOTES: stop_row.get(Columns.NOTES, ""),
        }

        recipient_dict = {}
        if stop_row.get(Columns.EMAIL) and not pd.isna(stop_row[Columns.EMAIL]):
            recipient_dict[CircuitColumns.EMAIL] = stop_row[Columns.EMAIL]
        if stop_row.get(Columns.PHONE) and not pd.isna(stop_row[Columns.PHONE]):
            recipient_dict[CircuitColumns.PHONE] = stop_row[Columns.PHONE]
        if stop_row.get(Columns.NAME) and not pd.isna(stop_row[Columns.NAME]):
            recipient_dict[CircuitColumns.NAME] = stop_row[Columns.NAME]
        stop[CircuitColumns.RECIPIENT] = recipient_dict

        stop_array.append(stop)

    return stop_array
