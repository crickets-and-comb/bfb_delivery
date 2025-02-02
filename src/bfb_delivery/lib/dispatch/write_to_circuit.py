"""Write routes to Circuit."""

import logging
from time import sleep

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from bfb_delivery.lib.constants import RateLimits

# TODO: Move _concat_response_pages to utils.
from bfb_delivery.lib.dispatch.read_circuit import _concat_response_pages
from bfb_delivery.lib.dispatch.utils import _get_response_dict, get_circuit_key, get_responses

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: Just wrap the CLI with split_chunked_routes now from the start.
# TODO: Default to soones Friday.
@typechecked
def upload_split_chunked(
    split_chunked_workbook_fp: str, start_date: str, distribute: bool, verbose: bool
) -> pd.DataFrame:
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
    """
    workbook = pd.ExcelFile(split_chunked_workbook_fp)
    sheet_plan_df = _create_plans(workbook=workbook, start_date=start_date, verbose=verbose)
    plan_ids = sheet_plan_df["plan_id"].tolist()
    _upload_stops(workbook=workbook, sheet_plan_df=sheet_plan_df)
    _optimize_routes(plan_ids=plan_ids)

    if distribute:
        _distribute_routes(plan_ids=plan_ids)

    sheet_plan_df["distributed"] = distribute
    sheet_plan_df["start_date"] = start_date

    return sheet_plan_df


@typechecked
def _create_plans(workbook: pd.ExcelFile, start_date: str, verbose: bool) -> pd.DataFrame:
    """Create a plan for each route in the split chunked workbook.

    Args:
        workbook: The Excel workbook.
        start_date: The date to start the routes, as "YYYY-MM-DD".
        verbose: Whether to print verbose output.

    Returns:
        A DataFrame with the plan IDs and driver IDs for each sheet.
    """
    route_driver_df = _get_driver_ids(workbook=workbook)
    sheet_plan_df = _initialize_plans(
        route_driver_df=route_driver_df, start_date=start_date, verbose=verbose
    )

    return sheet_plan_df


@typechecked
def _upload_stops(workbook: pd.ExcelFile, sheet_plan_df: pd.DataFrame) -> None:
    """Upload stops for each route in the split chunked workbook.

    Args:
        workbook: The Excel workbook.
        sheet_plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.
    """
    stop_arrays = _build_stop_arrays(workbook=workbook, sheet_plan_df=sheet_plan_df)
    _upload_stop_arrays(stop_arrays=stop_arrays)


@typechecked
def _optimize_routes(plan_ids: list[str]) -> None:
    """Optimize the routes."""
    for plan_id in plan_ids:
        _optimize_route(plan_id=plan_id)


@typechecked
def _distribute_routes(plan_ids: list[str]) -> None:
    """Distribute the routes."""
    for plan_id in plan_ids:
        _distribute_route(plan_id=plan_id)


@typechecked
def _get_driver_ids(workbook: pd.ExcelFile) -> pd.DataFrame:
    """Get the driver IDs for each sheet.

    Args:
        workbook: The Excel workbook.

    Returns:
        A DataFrame with the driver IDs for each sheet.
    """
    route_driver_df = pd.DataFrame(
        {"route_title": workbook.sheet_names, "driver_name": None, "email": None, "id": None}
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

            route_driver_df.loc[idx, ["plan_id", "depot", "writeable", "optimization"]] = (
                plan_response["id"],
                plan_response["depot"],
                plan_response["writable"],
                plan_response["optimization"],
            )
    except Exception as e:
        # Can't clean up plans if they're not finished.
        logger.error(f"Error initializing plans:\n{route_driver_df}")
        raise e

    null_depots = route_driver_df[route_driver_df["depot"].isnull()]
    if null_depots.not_empty:
        raise ValueError(f"Depot is null for the following routes:\n{null_depots}")
    not_writable = route_driver_df[~(route_driver_df["writeable"])]
    if not_writable.not_empty:
        raise ValueError(f"Plan is not writable for the following routes:\n{not_writable}")
    not_creating = route_driver_df[route_driver_df["optimization"] != "creating"]
    if not_creating.not_empty:
        raise ValueError(f"Plan is not creating for the following routes:\n{not_creating}")

    return route_driver_df


@typechecked
def _build_stop_arrays(
    workbook: pd.ExcelFile, sheet_plan_df: pd.DataFrame
) -> list[list[dict[str, dict[str, str] | list[str] | int | str]]]:
    """Build stop arrays for each route.

    Args:
        workbook: The Excel workbook
        sheet_plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.

    Returns:
        A list of stop arrays for batch stop uploads.
    """
    workbook = _parse_addresses(workbook=workbook)
    all_stops = _build_all_stops(workbook=workbook, sheet_plan_df=sheet_plan_df)
    stop_arrays = []
    # Split all_stops into chunks of 100 stops.
    number_of_stops = len(all_stops)
    for i in range(0, number_of_stops, 100):
        stop_arrays.append(
            all_stops[i : i + 100]  # noqa: E203
        )  # TODO: Add noqa E203 to shared, and remove throughout codebase.

    return stop_arrays


@typechecked
def _upload_stop_arrays(
    stop_arrays: list[list[dict[str, dict[str, str] | list[str] | int | str]]],
) -> None:
    """Upload the stop arrays.

    Args:
        stop_arrays: A list of stop arrays for batch stop uploads.
    """
    for stop_array in stop_arrays:
        _upload_stop_array(stop_array=stop_array)


@typechecked
def _optimize_route(plan_id: str) -> None:
    """Optimize a route."""
    pass


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
        response_dict = _get_response_dict(response=response)
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
            response_dict = _get_response_dict(response=response)
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
def _parse_addresses(workbook: pd.ExcelFile) -> pd.ExcelFile:
    """Parse addresses for each route."""
    pass


@typechecked
def _build_all_stops(
    workbook: pd.ExcelFile, sheet_plan_df: pd.DataFrame
) -> list[dict[str, dict[str, str] | list[str] | int | str]]:
    """Build all stops for each route.

    Args:
        workbook: The Excel workbook
        sheet_plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.

    Returns:
        A list of stops all the stops to upload.
    """
    pass


@typechecked
def _upload_stop_array(
    stop_array: list[dict[str, dict[str, str] | list[str] | int | str]],
) -> None:
    """Upload a stop array."""
    pass
