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
    # TODO: This isn't airtight still if we're not asking user about every sheet.
    # What if the sheet name is Hank, and the true driver is Henry, but there's also a Hank
    # in the DB? We wouldn't catch that, and would just assign Hank.
    # I think we should just ask the user for every sheet. We'd show a list of driver names
    # from the DB, each with an index number. Then ask for the number of the correct driver
    # for each sheet. If they want to see the list of drivers again, they can type "list".
    # If they want a list of best guesses, they can type "guesses".
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
    name_id_map = name_id_map[["id", "name", "driver_name", "email"]]
    name_id_map = name_id_map.drop_duplicates()
    name_id_map = _resolve_driver_name_conflicts(name_id_map=name_id_map)

    sheet_driver_df = sheet_driver_df.merge(name_id_map, on="driver_name", how="left")
    sheet_driver_df = _resolve_unmatched_sheets(
        sheet_driver_df=sheet_driver_df, drivers_df=drivers_df
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
    drivers_df = drivers_df.sort_values(by="name").reset_index(drop=True)

    return drivers_df


# TODO: Pull these input handlers apart for tests.
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
            name_id_map = _resolve_driver_name_conflict(
                driver_name=driver_name, name_id_map=name_id_map
            )

    remaining_dupes = name_id_map[name_id_map["driver_name"].duplicated(keep=False)][
        "driver_name"
    ].unique()

    if remaining_dupes.size > 0:
        raise ValueError(
            (
                "Multiple drivers found for the following names. "
                "Please resolve the conflicts manually and alert the developer:\n"
                f"{remaining_dupes}"
            )
        )

    return name_id_map


@typechecked
def _resolve_driver_name_conflict(
    name_id_map: pd.DataFrame, driver_name: str
) -> pd.DataFrame:
    """Resolve a driver name conflict."""
    options = name_id_map[name_id_map["driver_name"] == driver_name].reset_index()
    print(f"\nDriver name: {driver_name}")

    for idx, row in options.iterrows():
        print(f"{idx + 1}. {row['name']}, {row['email']} ({row['id']})")

    resolved = False
    while not resolved:
        try:
            # TODO: Wrap this for test mock.
            choice = input(r"Enter the number of the correct driver for '{driver_name}':")

        except ValueError:
            print("Invalid input. Please enter a number.")

        else:
            choice = choice if choice else "-1"
            try:
                choice = int(choice.strip()) - 1
            except ValueError:
                print("Invalid input. Please enter a number.")
            else:
                if 0 <= choice < len(options):
                    selected_id = options.iloc[choice]["id"]
                    name_id_map = name_id_map[
                        (name_id_map["driver_name"] != driver_name)
                        | (name_id_map["id"] == selected_id)
                    ]
                    resolved = True
                else:
                    print("Invalid selection. Please choose a valid number.")

    return name_id_map


@typechecked
def _resolve_unmatched_sheets(
    sheet_driver_df: pd.DataFrame, drivers_df: pd.DataFrame
) -> pd.DataFrame:
    """Resolve sheets that do not have a matched driver ID by asking the user.

    Args:
        sheet_driver_df: The DataFrame mapping sheets to drivers.
        drivers_df: The DataFrame containing all available drivers.

    Returns:
        A DataFrame with all sheetnames mapped to driver IDs for each sheet.
    """
    unmatched_sheets = sheet_driver_df[sheet_driver_df["id"].isnull()]

    if not unmatched_sheets.empty:
        print(
            "\nSome sheets do not have a matched driver. Please assign a driver:\n"
            "Here are all possible drivers:\n"
        )
        for idx, driver in drivers_df.iterrows():
            print(f"{idx + 1}. {driver['name']}, {driver['email']} ({driver['id']})")

        for _, row in unmatched_sheets.iterrows():
            _resolve_unmatched_sheet(
                row=row, drivers_df=drivers_df, sheet_driver_df=sheet_driver_df
            )

    remaining_unmatched = sheet_driver_df[sheet_driver_df["id"].isnull()][
        "sheet_name"
    ].tolist()
    if remaining_unmatched:
        raise ValueError(
            "No driver assigned for the following sheets. Please alert developer and "
            f"resolve manually:\n{remaining_unmatched}"
        )

    return sheet_driver_df


@typechecked
def _resolve_unmatched_sheet(
    row: pd.Series, drivers_df: pd.DataFrame, sheet_driver_df: pd.DataFrame
) -> None:
    best_guesses = pd.DataFrame()
    for name_part in row["sheet_name"].split(" ")[1:]:
        if name_part not in ["&", "AND"]:
            best_guesses = pd.concat(
                [
                    best_guesses,
                    drivers_df[drivers_df["name"].str.contains(name_part, case=False)],
                ]
            )
    best_guesses = best_guesses.drop_duplicates().sort_values(by="name")

    sheet_name = row["sheet_name"]
    print(f"\nSheet: {sheet_name}")
    print("Choose any of the possible drivers above, but here are some best guesses:\n")
    for idx, driver in best_guesses.iterrows():
        print(f"{idx + 1}. {driver['name']} {driver['email']} (ID: {driver['id']})")

    resolved = False
    while not resolved:
        try:
            # TODO: Wrap this for test mock.
            choice = input(r"Enter the number of the correct driver for '{sheet_name}':")
        except ValueError:
            print("Invalid input. Please enter a number.")
        else:
            choice = choice if choice else "-1"
            try:
                choice = int(choice.strip()) - 1
            except ValueError:
                print("Invalid input. Please enter a number.")
            else:
                if 0 <= choice < len(drivers_df):
                    selected_driver = drivers_df.iloc[choice]
                    sheet_driver_df.loc[
                        sheet_driver_df["sheet_name"] == sheet_name, ["id", "name"]
                    ] = [selected_driver["id"], selected_driver["name"]]
                    resolved = True
                else:
                    print("Invalid selection. Please choose a valid number.")


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
