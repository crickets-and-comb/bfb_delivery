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
    drivers_df = _get_all_drivers()
    sheet_driver_df = pd.DataFrame({"sheet_name": workbook.sheet_names})

    sheet_driver_df["driver_name"] = sheet_driver_df["sheet_name"].apply(
        lambda sheet_name: " ".join(sheet_name.split(" ")[1:])
        .upper()
        .split("#")[0]
        .strip()
        .replace(" AND ", "& ")
    )
    drivers_df["name"] = drivers_df["name"].apply(
        lambda name: name.upper().strip().replace(" AND ", "& ")
    )

    name_id_map = pd.DataFrame()
    for driver_name in sheet_driver_df["driver_name"]:
        driver_ids_df = drivers_df[drivers_df["name"].str.startswith(driver_name)]
        driver_ids_df["driver_name"] = driver_name
        name_id_map = pd.concat([name_id_map, driver_ids_df], ignore_index=True)
    name_id_map = name_id_map[["id", "name", "driver_name"]]
    name_id_map = name_id_map.drop_duplicates()
    _resolve_driver_name_conflicts(name_id_map=name_id_map)

    sheet_driver_df = sheet_driver_df.merge(name_id_map, on="driver_name", how="left")

    sheets_without_drivers = sheet_driver_df[sheet_driver_df["id"].isnull()][
        "sheet_name"
    ].tolist()
    if sheets_without_drivers:
        raise ValueError(
            (
                "No driver found for the following sheets. Please ensure the sheet names "
                f"match the driver names in Circuit: {sheets_without_drivers}"
            )
        )

    inactive_drivers = sheet_driver_df[sheet_driver_df["id"].isnull()]["name"].tolist()
    if inactive_drivers and ignore_inactive_drivers is False:
        raise ValueError(
            (
                "Inactive drivers. Please activate the following drivers before creating "
                f"routes for them: {inactive_drivers}"
            )
        )

    return sheet_driver_df


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

    return drivers_df


@typechecked
def _resolve_driver_name_conflicts(name_id_map: pd.DataFrame) -> pd.DataFrame:
    """Resolve driver name conflicts with user input.

    Args:
        name_id_map: The DataFrame with the driver IDs for each sheet.

    Returns:
        A DataFrame with the driver IDs for each sheet.
    """
    duplicate_name_id_map_names = name_id_map[
        name_id_map["driver_name"].duplicated(keep=False)
    ]["driver_name"].unique()

    if duplicate_name_id_map_names.size > 0:
        print(
            "\nMultiple drivers found for the following names. Please select the correct one:"
        )

        for driver_name in duplicate_name_id_map_names:
            options = name_id_map[name_id_map["driver_name"] == driver_name].reset_index()
            print(f"\nDriver name: {driver_name}")

            for idx, row in options.iterrows():
                print(f"{idx + 1}. {row['name']} (ID: {row['id']})")

            while True:
                try:
                    choice = (
                        int(
                            input(
                                r"Enter the number of the correct driver for '{driver_name}':"
                            ).strip()
                        )
                        - 1
                    )

                    if 0 <= choice < len(options):
                        selected_id = options.iloc[choice]["id"]
                        name_id_map = name_id_map[
                            (name_id_map["driver_name"] != driver_name)
                            | (name_id_map["id"] == selected_id)
                        ]
                        break
                    else:
                        print("Invalid selection. Please choose a valid number.")

                except ValueError:
                    print("Invalid input. Please enter a number.")

    duplicate_name_id_map_names = name_id_map[
        name_id_map["driver_name"].duplicated(keep=False)
    ]["driver_name"].unique()

    if duplicate_name_id_map_names.size > 0:
        raise ValueError(
            (
                "Multiple drivers found for the following names. "
                "Please resolve the conflicts manually and alert the developer:\n"
                f"{duplicate_name_id_map_names}"
            )
        )

    return name_id_map


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
