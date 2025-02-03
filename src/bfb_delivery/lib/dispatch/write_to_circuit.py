"""Write routes to Circuit."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from typeguard import typechecked

from bfb_delivery.lib.constants import (
    CIRCUIT_DATE_FORMAT,
    MANIFEST_DATE_FORMAT,
    CircuitColumns,
    Columns,
    DocStrings,
)
from bfb_delivery.lib.dispatch.api_callers import (
    OptimizationChecker,
    OptimizationLauncher,
    PlanDistributor,
    PlanInitializer,
    StopUploader,
)

# TODO: Move _concat_response_pages to utils.
from bfb_delivery.lib.dispatch.read_circuit import _concat_response_pages, get_route_files
from bfb_delivery.lib.dispatch.utils import get_responses
from bfb_delivery.lib.formatting import sheet_shaping
from bfb_delivery.lib.utils import get_friday

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: Set defaults only at public and CLI, via constants.
# TODO: Set docstring via DocStrings.
@typechecked
def build_routes_from_chunked(  # noqa: D103
    input_path: str,
    output_dir: str,
    start_date: str,
    no_distribute: bool,
    verbose: bool,
    book_one_drivers_file: str,
    extra_notes_file: str,
) -> Path:
    start_date = start_date or get_friday(fmt=CIRCUIT_DATE_FORMAT)
    output_dir = output_dir if output_dir else f"./deliveries_{start_date}"
    Path(output_dir).mkdir(exist_ok=True)
    split_chunked_output_dir = Path(output_dir) / "split_chunked"
    split_chunked_output_dir.mkdir(exist_ok=True)

    split_chunked_workbook_fp = sheet_shaping.split_chunked_route(
        input_path=input_path,
        output_dir=split_chunked_output_dir,
        output_filename="",
        n_books=1,
        book_one_drivers_file=str(book_one_drivers_file) if book_one_drivers_file else "",
        date=datetime.strptime(start_date, CIRCUIT_DATE_FORMAT).strftime(
            MANIFEST_DATE_FORMAT
        ),
    )[0]

    sheet_plan_df = upload_split_chunked(
        split_chunked_workbook_fp=split_chunked_workbook_fp,
        start_date=start_date,
        no_distribute=no_distribute,
        verbose=verbose,
    )

    circuit_output_dir = get_route_files(
        start_date=start_date,
        end_date=start_date,
        plan_ids=sheet_plan_df["plan_id"].to_list(),
        output_dir="",
        all_hhs=False,  # Overridden by plan_ids.
        verbose=verbose,
    )
    final_manifest_path = sheet_shaping.create_manifests(
        input_dir=circuit_output_dir,
        output_dir=output_dir,
        output_filename="",
        extra_notes_file=str(extra_notes_file) if extra_notes_file else "",
    )

    return final_manifest_path


build_routes_from_chunked.__doc__ = DocStrings.BUILD_ROUTES_FROM_CHUNKED.api_docstring


@typechecked
def upload_split_chunked(
    split_chunked_workbook_fp: Path, start_date: str, no_distribute: bool, verbose: bool
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
        no_distribute: To skip distributing the routes after optimizing.
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
        stops_df = stops_df.fillna("")
        # TODO: For each step, if some succeed and others do not, continue with the
        # successful ones and add the statuses to the sheet_plan_df for a final report.
        sheet_plan_df = _create_plans(
            stops_df=stops_df, start_date=start_date, verbose=verbose
        )
        _upload_stops(stops_df=stops_df, sheet_plan_df=sheet_plan_df, verbose=verbose)
        _optimize_routes(sheet_plan_df=sheet_plan_df, verbose=verbose)

        if not no_distribute:
            # TODO: Return map of distribution statuses in case of error.
            # And continue with final printout.
            _distribute_routes(sheet_plan_df=sheet_plan_df, verbose=verbose)

        sheet_plan_df["distributed"] = not no_distribute
        sheet_plan_df["start_date"] = start_date

    return sheet_plan_df


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
def _upload_stops(stops_df: pd.DataFrame, sheet_plan_df: pd.DataFrame, verbose: bool) -> None:
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
    errors = {}
    for plan_id, stop_arrays in plan_stops.items():
        plan_title = sheet_plan_df[sheet_plan_df["plan_id"] == plan_id]["route_title"].values[
            0
        ]

        if verbose:
            logger.info(f"Uploading stops for {plan_title} ({plan_id}) ...")

        for stop_array in stop_arrays:
            stop_uploader = StopUploader(
                plan_id=plan_id, plan_title=plan_title, stop_array=stop_array
            )
            try:
                stop_uploader.call_api()
            except Exception as e:
                logger.error(f"Error uploading stops for {plan_title} ({plan_id}):\n{e}")
                if plan_id not in errors:
                    errors[plan_title] = [e]
                else:
                    errors[plan_title].append(e)
            else:
                uploaded_stops[plan_title] = stop_uploader.stop_ids
                stop_id_count += len(stop_uploader.stop_ids)

    logger.info(
        f"Finished uploading stops. Uploaded {stop_id_count} stops for "
        f"{len(uploaded_stops)} plans."
    )

    if errors:
        raise RuntimeError(f"Errors uploading stops:\n{errors}")

    return


@typechecked
def _optimize_routes(sheet_plan_df: pd.DataFrame, verbose: bool) -> None:
    """Optimize the routes."""
    logger.info("Initializing route optimizations ...")
    plan_ids = sheet_plan_df["plan_id"].to_list()
    optimizations = {}

    errors = {}
    for plan_id in plan_ids:
        plan_title = sheet_plan_df[sheet_plan_df["plan_id"] == plan_id]["route_title"].values[
            0
        ]
        if verbose:
            logger.info(f"Optimizing route for {plan_title} ({plan_id}) ...")
        # TODO: If optimization comes back finished on launch, don't check later.
        optimization = OptimizationLauncher(plan_id=plan_id, plan_title=plan_title)
        try:
            optimization.call_api()
        except Exception as e:
            logger.error(f"Error launching optimization for {plan_title} ({plan_id}):\n{e}")
            errors[plan_id] = e

        else:
            optimizations[plan_id] = optimization.operation_id
            if verbose:
                logger.info(
                    f"Launched optimization for {plan_title} ({plan_id}): "
                    f"{optimization.operation_id}"
                )

    logger.info(
        "Finished initializing route optimizations: for "
        f"{len(sheet_plan_df[~(sheet_plan_df['plan_id']).isin(errors.keys())])} plans."
    )

    if errors:
        raise RuntimeError(f"Errors launching optimizations:\n{errors}")

    _wait_for_optimizations(
        sheet_plan_df=sheet_plan_df, optimizations=optimizations, verbose=verbose
    )

    return


# TODO: Make a CLI for this since it will be optional.
@typechecked
def _distribute_routes(sheet_plan_df: pd.DataFrame, verbose: bool) -> None:
    """Distribute the routes."""
    logger.info("Distributing routes ...")
    errors = {}
    for plan_id in sheet_plan_df["plan_id"].to_list():
        plan_title = sheet_plan_df[sheet_plan_df["plan_id"] == plan_id]["route_title"].values[
            0
        ]
        if verbose:
            logger.info(f"Distributing plan for {plan_title} ({plan_id}) ...")
        distributor = PlanDistributor(plan_id=plan_id, plan_title=plan_title)
        try:
            distributor.call_api()
        except Exception as e:
            logger.error(f"Error distributing plan for {plan_title} ({plan_id}):\n{e}")
            errors[plan_id] = e

    logger.info(
        "Finished distributing routes: for "
        f"{len(sheet_plan_df[~(sheet_plan_df["plan_id"]).isin(errors.keys())])} plans."
    )

    if errors:
        raise RuntimeError(f"Errors distributing plans:\n{errors}")


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
    errors = {}
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
        plan_initializer = PlanInitializer(plan_data=plan_data)
        try:
            plan_initializer.call_api()
        except Exception as e:
            logger.error(f"Error initializing plan for {row['route_title']}:\n{e}")
            errors[row["route_title"]] = e
        else:
            if verbose:
                logger.info(
                    f"Created plan {plan_initializer._response_json['id']} for "
                    f"{row['route_title']}.\n{plan_initializer._response_json}"
                )

            route_driver_df.loc[idx, ["plan_id", "writeable"]] = (
                plan_initializer._response_json["id"],
                plan_initializer._response_json["writable"],
            )

    logger.info(f"Finished initializing plans. Initialized {idx + 1 - len(errors)} plans.")

    if errors:
        raise RuntimeError(f"Errors initializing plans:\n{errors}")

    not_writable = route_driver_df[route_driver_df["writeable"] == False]  # noqa: E712
    if not not_writable.empty:
        raise ValueError(f"Plan is not writable for the following routes:\n{not_writable}")

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
def _wait_for_optimizations(
    sheet_plan_df: pd.DataFrame, optimizations: dict[str, str], verbose: bool
) -> None:
    """Wait for all optimizations to finish."""
    logger.info("Waiting for optimizations to finish ...")
    optimizations_finished: dict[str, bool | str] = {
        plan_id: False for plan_id in sheet_plan_df["plan_id"].to_list()
    }
    errors = {}
    while not all([val is True or val == "error" for val in optimizations_finished.values()]):
        unfinished = [
            plan_id
            for plan_id, finished in optimizations_finished.items()
            if not finished or finished != "error"
        ]
        for plan_id in unfinished:
            plan_title = sheet_plan_df[sheet_plan_df["plan_id"] == plan_id][
                "route_title"
            ].values[0]
            if verbose:
                logger.info(f"Checking optimization for {plan_title} ({plan_id}) ...")
            check_op = OptimizationChecker(
                plan_id=plan_id, operation_id=optimizations[plan_id], plan_title=plan_title
            )
            try:
                check_op.call_api()
            except Exception as e:
                logger.error(
                    f"Error checking optimization for {plan_title} ({plan_id}):\n{e}"
                )
                errors[plan_id] = [e]

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
        }

        if stop_row.get(Columns.NOTES) and not pd.isna(stop_row[Columns.NOTES]):
            stop[CircuitColumns.NOTES] = stop_row[Columns.NOTES]

        recipient_dict = {}
        if stop_row.get(Columns.EMAIL) and not pd.isna(stop_row[Columns.EMAIL]):
            recipient_dict[CircuitColumns.EMAIL] = stop_row[Columns.EMAIL]
        if stop_row.get(Columns.PHONE) and not pd.isna(stop_row[Columns.PHONE]):
            recipient_dict[CircuitColumns.PHONE] = stop_row[Columns.PHONE]
        if stop_row.get(Columns.NAME) and not pd.isna(stop_row[Columns.NAME]):
            recipient_dict[CircuitColumns.NAME] = stop_row[Columns.NAME]
        if recipient_dict:
            stop[CircuitColumns.RECIPIENT] = recipient_dict

        stop_array.append(stop)

    return stop_array
