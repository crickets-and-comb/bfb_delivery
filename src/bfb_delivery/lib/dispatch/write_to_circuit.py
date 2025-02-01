"""Write routes to Circuit."""

import logging

import pandas as pd
from typeguard import typechecked

# TODO: Move _concat_response_pages to utils.
from bfb_delivery.lib.dispatch.read_circuit import _concat_response_pages
from bfb_delivery.lib.dispatch.utils import get_responses

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: Just wrap the CLI with split_chunked_routes now from the start.
@typechecked
def upload_split_chunked(
    split_chunked_workbook_fp: str,
    start_date: str,
    distribute: bool = False,
    ignore_inactive_drivers: bool = False,
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


    Returns:
        A DataFrame with the plan IDs and driver IDs for each sheet,
            along with date and whether distributed.
    """
    workbook = pd.ExcelFile(split_chunked_workbook_fp)
    sheet_plan_df = _create_plans(
        workbook=workbook, ignore_inactive_drivers=ignore_inactive_drivers
    )
    plan_ids = sheet_plan_df["plan_id"].tolist()
    _upload_stops(workbook=workbook, sheet_plan_df=sheet_plan_df)
    _optimize_routes(plan_ids=plan_ids)

    if distribute:
        _distribute_routes(plan_ids=plan_ids)

    sheet_plan_df["distributed"] = distribute
    sheet_plan_df["start_date"] = start_date

    return sheet_plan_df


@typechecked
def _create_plans(workbook: pd.ExcelFile, ignore_inactive_drivers: bool) -> pd.DataFrame:
    """Create a plan for each route in the split chunked workbook."""
    sheet_driver_df = _get_driver_ids(
        workbook=workbook, ignore_inactive_drivers=ignore_inactive_drivers
    )
    sheet_plan_df = _initialize_plans(sheet_driver_df=sheet_driver_df)

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
def _get_driver_ids(workbook: pd.ExcelFile, ignore_inactive_drivers: bool) -> pd.DataFrame:
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
    if ignore_inactive_drivers is False:
        inactive_drivers = drivers_df[~(drivers_df["active"])]["name"].tolist()
        if inactive_drivers:
            raise ValueError(
                (
                    "Inactive drivers. Please activate the following drivers before creating "
                    f"routes for them: {inactive_drivers}"
                )
            )

    route_driver_df = _assign_drivers(drivers_df=drivers_df, route_driver_df=route_driver_df)

    return route_driver_df


@typechecked
def _initialize_plans(sheet_driver_df: pd.DataFrame) -> pd.DataFrame:
    """Initialize plans for each driver.

    Args:
        sheet_driver_df: The DataFrame with the driver IDs for each sheet.

    Returns:
        A DataFrame with the plan IDs and driver IDs for each sheet.
    """
    pass


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
def _assign_driver(
    route_title: str, drivers_df: pd.DataFrame, route_driver_df: pd.DataFrame
) -> pd.DataFrame:
    """Ask user to assign driver to each route."""
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
                    ["id", "driver_name", "email"],
                ] = [
                    drivers_df.iloc[choice]["id"],
                    drivers_df.iloc[choice]["name"],
                    drivers_df.iloc[choice]["email"],
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
