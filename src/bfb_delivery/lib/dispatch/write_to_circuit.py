"""Write routes to Circuit."""

import logging
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from typeguard import typechecked

from bfb_delivery.lib import errors, schema
from bfb_delivery.lib.constants import (
    CIRCUIT_DATE_FORMAT,
    CIRCUIT_DRIVERS_URL,
    MANIFEST_DATE_FORMAT,
    CircuitColumns,
    Columns,
    DocStrings,
    IntermediateColumns,
    RateLimits,
)
from bfb_delivery.lib.dispatch.api_callers import (
    OptimizationChecker,
    OptimizationLauncher,
    PlanDeleter,
    PlanDistributor,
    PlanInitializer,
    StopUploader,
)
from bfb_delivery.lib.dispatch.read_circuit import get_route_files
from bfb_delivery.lib.dispatch.utils import concat_response_pages, get_responses
from bfb_delivery.lib.formatting.sheet_shaping import create_manifests, split_chunked_route
from bfb_delivery.lib.schema.utils import schema_error_handler
from bfb_delivery.lib.utils import get_friday

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@typechecked
def build_routes_from_chunked(  # noqa: D103
    input_path: str,
    output_dir: str,
    start_date: str,
    no_distribute: bool,
    verbose: bool,
    extra_notes_file: str,
) -> Path:
    start_date = start_date or get_friday(fmt=CIRCUIT_DATE_FORMAT)

    input_path = str(Path(input_path).resolve())
    output_dir = output_dir if output_dir else f"./deliveries_{start_date}"
    output_dir = str(Path(output_dir).resolve())
    Path(output_dir).mkdir(exist_ok=True)
    split_chunked_output_dir = Path(output_dir) / "split_chunked"
    split_chunked_output_dir.mkdir(exist_ok=True)

    split_chunked_workbook_fp = split_chunked_route(
        input_path=input_path,
        output_dir=split_chunked_output_dir,
        output_filename="",
        n_books=1,
        book_one_drivers_file="",
        date=datetime.strptime(start_date, CIRCUIT_DATE_FORMAT).strftime(
            MANIFEST_DATE_FORMAT
        ),
    )[0]

    plan_df = upload_split_chunked(
        split_chunked_workbook_fp=split_chunked_workbook_fp,
        output_dir=Path(output_dir),
        start_date=start_date,
        no_distribute=no_distribute,
        verbose=verbose,
    )

    successful_plans = plan_df[plan_df[IntermediateColumns.OPTIMIZED] == True][  # noqa: E712
        IntermediateColumns.PLAN_ID
    ].to_list()

    circuit_output_dir = get_route_files(
        start_date=start_date,
        end_date=start_date,
        plan_ids=successful_plans,
        output_dir=output_dir,
        verbose=verbose,
    )

    final_manifest_path = create_manifests(
        input_dir=circuit_output_dir,
        output_dir=output_dir,
        output_filename="",
        extra_notes_file=str(extra_notes_file) if extra_notes_file else "",
    )

    # TODO: Validate that input data matches ouput data.
    # (Ignore skipped routes, and account for imputation, caps, etc.)

    return final_manifest_path


build_routes_from_chunked.__doc__ = DocStrings.BUILD_ROUTES_FROM_CHUNKED.api_docstring


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def upload_split_chunked(
    split_chunked_workbook_fp: Path,
    output_dir: Path,
    start_date: str,
    no_distribute: bool,
    verbose: bool,
) -> DataFrame[schema.PlansUploadSplitChunkedOut]:
    """Upload, optimize, and distribute a split chunked Excel workbook of routes to Circuit.

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
        output_dir: A place to put intermediate files.
        start_date: The date to start the routes, as "YYYY-MM-DD".
        no_distribute: To skip distributing the routes after optimizing.
        verbose: Whether to print verbose output.

    Returns:
        A DataFrame with the plan IDs and driver IDs for each sheet,
            along with date and statuses at each step.
    """
    plan_output_dir = output_dir / "plans"
    plan_output_dir.mkdir(exist_ok=True)
    plan_df_path = plan_output_dir / "plans.csv"
    stops_df_path = plan_output_dir / "stops.csv"

    stops_df = _create_stops_df(
        split_chunked_workbook_fp=split_chunked_workbook_fp, stops_df_path=stops_df_path
    )

    plan_df = _create_plans(
        stops_df=stops_df, start_date=start_date, plan_df_path=plan_df_path, verbose=verbose
    )
    plan_df.to_csv(plan_df_path, index=False)

    plan_df = _upload_stops(stops_df=stops_df, plan_df=plan_df, verbose=verbose)
    plan_df.to_csv(plan_df_path, index=False)

    plan_df = _optimize_routes(plan_df=plan_df, verbose=verbose)
    plan_df.to_csv(plan_df_path, index=False)

    if not no_distribute:
        plan_df = _distribute_routes(plan_df=plan_df, verbose=verbose)
    else:
        plan_df[CircuitColumns.DISTRIBUTED] = False

    plan_df[IntermediateColumns.START_DATE] = start_date
    plan_df.to_csv(plan_df_path, index=False)

    _print_report(plan_df=plan_df, no_distribute=no_distribute)

    return plan_df


@typechecked
def delete_plans(plan_ids: list[str], plan_df_fp: str) -> list[str]:
    """Delete plans from Circuit.

    Args:
        plan_ids: The plan IDs to delete.
        plan_df_fp: The file path to a dataframe with plan IDs to be deleted
            in column 'plan_id'.

    Returns:
        The plan IDs that were (to be) deleted.

    Raises:
        ValueError: If both plan_ids and plan_df_fp are provided.
        ValueError: If neither plan_ids nor plan_df_fp are provided.
        RuntimeError: If there are errors deleting plans.
    """
    if plan_ids and plan_df_fp:
        raise ValueError("Please provide either plan_ids or plan_ids_fp, not both.")
    if not plan_ids and not plan_df_fp:
        raise ValueError("Please provide either plan_ids or plan_ids_fp.")

    if plan_df_fp:
        plan_df = pd.read_csv(plan_df_fp)
        plan_ids = plan_df[IntermediateColumns.PLAN_ID].to_list()

    logger.info(f"Deleting plans: {plan_ids} ...")

    errors = {}
    for plan_id in plan_ids:
        try:
            deleted = delete_plan(plan_id=plan_id)
        except Exception as e:
            errors[plan_id] = e
            logger.error(f"Error deleting plan {plan_id}.")
        else:
            logger.info(f"Plan {plan_id} deleted: {deleted}")

    logger.info("Finished deleting plans.")

    if errors:
        raise RuntimeError(f"Errors deleting plans:\n{errors}")

    return plan_ids


@typechecked
def delete_plan(plan_id: str) -> bool:
    """Delete a plan from Circuit.

    Args:
        plan_id: The plan ID to be deleted.

    Returns:
        Whether the plan was deleted. (Should always be True if no errors.)
    """
    deleter = PlanDeleter(plan_id=plan_id)
    deleter.call_api()

    return deleter.deletion


@typechecked
def _create_stops_df(split_chunked_workbook_fp: Path, stops_df_path: Path) -> pd.DataFrame:
    stops_dfs = []
    with pd.ExcelFile(split_chunked_workbook_fp) as workbook:
        for sheet in workbook.sheet_names:
            df = workbook.parse(sheet)
            df[IntermediateColumns.SHEET_NAME] = str(sheet)
            stops_dfs.append(df)
    stops_df = pd.concat(stops_dfs).reset_index(drop=True)
    stops_df = stops_df.fillna("")
    stops_df.to_csv(stops_df_path, index=False)

    return stops_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _create_plans(
    stops_df: DataFrame[schema.Stops], start_date: str, plan_df_path: Path, verbose: bool
) -> DataFrame[schema.PlansCreatePlansOut]:
    """Create a Circuit plan for each route.

    Args:
        stops_df: The long DataFrame with all the routes.
        start_date: The date to start the routes, as "YYYY-MM-DD".
        plan_df_path: The file path to save the plan DataFrame in case of error.
            Helps to automate cleanup.
        verbose: Whether to print verbose output.
    """
    plan_df = _assign_drivers_to_plans(stops_df=stops_df)
    try:
        plan_df = _initialize_plans(plan_df=plan_df, start_date=start_date, verbose=verbose)
    # TODO: Validate schema before returning, write before raising within _initialize_plans.
    except Exception as e:
        plan_df.to_csv(plan_df_path, index=False)
        raise e

    return plan_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _upload_stops(
    stops_df: DataFrame[schema.Stops],
    plan_df: DataFrame[schema.PlansUploadStopsIn],
    verbose: bool,
) -> DataFrame[schema.PlansUploadStopsOut]:
    """Upload stops for each route.

    Will skip plans marked as not initialized.

    Args:
        stops_df: The long DataFrame with all the routes.
        plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.
        verbose: Whether to print verbose output.

    Returns:
        A dataframe of the plans with a column for whether stops were uploaded successfully.
    """
    plan_stops = _build_plan_stops(
        stops_df=stops_df,
        plan_df=plan_df[plan_df[IntermediateColumns.INITIALIZED] == True],  # noqa: E712
    )

    logger.info(
        f"Uploading stops. Allow {RateLimits.BATCH_STOP_IMPORT_SECONDS}+ seconds per plan ..."
    )
    uploaded_stops = {}
    stop_id_count = 0
    errors = {}
    for plan_id, stop_arrays in plan_stops.items():
        plan_title = plan_df[plan_df[IntermediateColumns.PLAN_ID] == plan_id][
            IntermediateColumns.ROUTE_TITLE
        ].values[0]

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

    plan_df[IntermediateColumns.STOPS_UPLOADED] = False
    plan_df.loc[
        (plan_df[IntermediateColumns.PLAN_ID].isin(plan_stops.keys()))
        & ~(plan_df[IntermediateColumns.ROUTE_TITLE].isin(errors.keys())),
        IntermediateColumns.STOPS_UPLOADED,
    ] = True
    if errors:
        logger.warning(f"Errors uploading stops:\n{errors}")

    return plan_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _optimize_routes(
    plan_df: DataFrame[schema.PlansOptimizeRoutesIn], verbose: bool
) -> DataFrame[schema.PlansOptimizeRoutesOut]:
    """Optimize the routes.

    Will skip plans marked as without stops uploaded.

    Args:
        plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.
        verbose: Whether to print verbose output.

    Returns:
        A DataFrame of the plans with a column for whether the routes were optimized.
    """
    logger.info(
        "Initializing route optimizations. "
        f"Allow {RateLimits.OPTIMIZATION_PER_SECOND}+ seconds per plan ..."
    )
    plan_ids = plan_df[plan_df[IntermediateColumns.STOPS_UPLOADED] == True][  # noqa: E712
        IntermediateColumns.PLAN_ID
    ].to_list()
    optimizations = {}

    errors = {}
    for plan_id in plan_ids:
        plan_title = plan_df[plan_df[IntermediateColumns.PLAN_ID] == plan_id][
            IntermediateColumns.ROUTE_TITLE
        ].values[0]
        if verbose:
            logger.info(f"Optimizing route for {plan_title} ({plan_id}) ...")

        optimization = OptimizationLauncher(plan_id=plan_id, plan_title=plan_title)
        try:
            optimization.call_api()
        except Exception as e:
            logger.error(f"Error launching optimization for {plan_title} ({plan_id}):\n{e}")
            errors[plan_title] = e

        else:
            optimizations[plan_id] = optimization.operation_id
            if verbose:
                logger.info(
                    f"Launched optimization for {plan_title} ({plan_id}): "
                    f"{optimization.operation_id}"
                )

    logger.info(
        "Finished initializing route optimizations for "
        f"{len(plan_df[~(plan_df[IntermediateColumns.ROUTE_TITLE]).isin(errors.keys())])} "
        "plans."
    )

    plan_df[IntermediateColumns.OPTIMIZED] = False
    plan_df.loc[
        (plan_df[IntermediateColumns.PLAN_ID].isin(plan_ids))
        & ~(plan_df[IntermediateColumns.ROUTE_TITLE].isin(errors.keys())),
        IntermediateColumns.OPTIMIZED,
    ] = True
    if errors:
        logger.warning(f"Errors launching optimizations:\n{errors}")

    plan_df = _confirm_optimizations(
        plan_df=plan_df, optimizations=optimizations, verbose=verbose
    )

    return plan_df


# TODO: Make a CLI for this since it will be optional.
@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _distribute_routes(
    plan_df: DataFrame[schema.PlansDistributeRoutesIn], verbose: bool
) -> DataFrame[schema.PlansDistributeRoutesOut]:
    """Distribute the routes.

    Will skip plans marked as not initialized.

    Args:
        plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.
        verbose: Whether to print verbose output.

    Returns:
        A DataFrame of the plans with a column for whether the routes were distributed.
    """
    logger.info("Distributing routes ...")

    plan_ids = plan_df[plan_df[IntermediateColumns.OPTIMIZED] == True][  # noqa: E712
        IntermediateColumns.PLAN_ID
    ].to_list()
    errors = {}
    for plan_id in plan_ids:
        plan_title = plan_df[plan_df[IntermediateColumns.PLAN_ID] == plan_id][
            IntermediateColumns.ROUTE_TITLE
        ].values[0]
        if verbose:
            logger.info(f"Distributing plan for {plan_title} ({plan_id}) ...")
        distributor = PlanDistributor(plan_id=plan_id, plan_title=plan_title)
        try:
            distributor.call_api()
        except Exception as e:
            logger.error(f"Error distributing plan for {plan_title} ({plan_id}):\n{e}")
            errors[plan_title] = e

    logger.info(
        "Finished distributing routes for "
        f"{len(plan_df[~(plan_df[IntermediateColumns.ROUTE_TITLE]).isin(errors.keys())])} "
        "plans."
    )
    plan_df[CircuitColumns.DISTRIBUTED] = False
    plan_df.loc[
        (plan_df[IntermediateColumns.PLAN_ID].isin(plan_ids))
        & ~(plan_df[IntermediateColumns.ROUTE_TITLE].isin(errors.keys())),
        CircuitColumns.DISTRIBUTED,
    ] = True
    if errors:
        logger.warning(f"Errors distributing plans:\n{errors}")

    return plan_df


@typechecked
def _print_report(plan_df: pd.DataFrame, no_distribute: bool) -> None:
    """Print a report of upload results."""
    plans_attempted = plan_df[IntermediateColumns.ROUTE_TITLE].to_list()
    plans_initialized = plan_df[
        plan_df[IntermediateColumns.INITIALIZED] == True  # noqa: E712
    ][IntermediateColumns.ROUTE_TITLE].to_list()
    plans_with_stops = plan_df[
        plan_df[IntermediateColumns.STOPS_UPLOADED] == True  # noqa: E712
    ][IntermediateColumns.ROUTE_TITLE].to_list()
    plans_optimized = plan_df[plan_df[IntermediateColumns.OPTIMIZED] == True][  # noqa: E712
        IntermediateColumns.ROUTE_TITLE
    ].to_list()
    plans_distributed = plan_df[plan_df[CircuitColumns.DISTRIBUTED] == True][  # noqa: E712
        IntermediateColumns.ROUTE_TITLE
    ].to_list()
    report_df = plan_df[
        [
            IntermediateColumns.ROUTE_TITLE,
            IntermediateColumns.INITIALIZED,
            CircuitColumns.WRITABLE,
            IntermediateColumns.STOPS_UPLOADED,
            IntermediateColumns.OPTIMIZED,
            CircuitColumns.DISTRIBUTED,
        ]
    ]
    logger.info(
        f"\n{report_df}\n"
        f"\nPlans attempted: {len(plans_attempted)}\n"
        f"Plans initialized: {len(plans_initialized)}\n"
        f"Plans with stops: {len(plans_with_stops)}\n"
        f"Plans optimized: {len(plans_optimized)}\n"
        f"Plans distributed: {len(plans_distributed)}"
    )
    if not no_distribute and len(plans_attempted) != len(plans_distributed):
        plans_not_distributed = list(set(plans_attempted) - set(plans_distributed))
        logger.warning(
            "Not all plans were distributed. "
            f"These are the undistributed plans:\n{plans_not_distributed}\n"
            "See the above output to see at which steps the plans failed."
        )
    elif no_distribute and len(plans_attempted) != len(plans_optimized):
        plans_not_optimized = list(set(plans_attempted) - set(plans_optimized))
        logger.warning(
            "Not all plans were optimized. "
            f"These are the non-optimized plans:\n{plans_not_optimized}\n"
            "See the above output to see at which steps the plans failed."
        )


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _assign_drivers_to_plans(
    stops_df: DataFrame[schema.Stops],
) -> DataFrame[schema.PlansAssignDriversToPlansOut]:
    """Get the driver IDs for each sheet.

    Args:
        stops_df: The long DataFrame with all the routes.

    Returns:
        A DataFrame with the driver IDs for each sheet.
    """
    plan_df = pd.DataFrame(
        {
            IntermediateColumns.ROUTE_TITLE: stops_df[
                IntermediateColumns.SHEET_NAME
            ].unique(),
            IntermediateColumns.DRIVER_NAME: None,
            CircuitColumns.EMAIL: None,
            CircuitColumns.ID: None,
        }
    )

    drivers_df = _get_all_drivers()
    plan_df = _assign_drivers(drivers_df=drivers_df, plan_df=plan_df)
    inactive_drivers = plan_df[~(plan_df[CircuitColumns.ACTIVE])][
        IntermediateColumns.DRIVER_NAME
    ].tolist()
    if inactive_drivers:
        raise ValueError(
            (
                "Inactive drivers. Please activate the following drivers before creating "
                f"routes for them: {inactive_drivers}"
            )
        )

    return plan_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _initialize_plans(
    plan_df: DataFrame[schema.PlansInitializePlansIn], start_date: str, verbose: bool
) -> DataFrame[schema.PlansInitializePlansOut]:
    """Initialize Circuit plans with drivers."""
    plan_df[IntermediateColumns.PLAN_ID] = None
    plan_df[CircuitColumns.WRITABLE] = None
    # TODO: Do we need this column?
    plan_df[CircuitColumns.OPTIMIZATION] = None

    logger.info("Initializing plans ...")
    errors = {}
    for idx, row in plan_df.iterrows():
        plan_data = {
            CircuitColumns.TITLE: row[IntermediateColumns.ROUTE_TITLE],
            # TODO: Just make this once.
            CircuitColumns.STARTS: {
                CircuitColumns.DAY: int(start_date.split("-")[2]),
                CircuitColumns.MONTH: int(start_date.split("-")[1]),
                CircuitColumns.YEAR: int(start_date.split("-")[0]),
            },
            CircuitColumns.DRIVERS: [row[CircuitColumns.ID]],
        }
        if verbose:
            logger.info(f"Creating plan for {row[IntermediateColumns.ROUTE_TITLE]} ...")
        plan_initializer = PlanInitializer(plan_data=plan_data)
        try:
            plan_initializer.call_api()
        except Exception as e:
            logger.error(
                f"Error initializing plan for {row[IntermediateColumns.ROUTE_TITLE]}:\n{e}"
            )
            errors[row[IntermediateColumns.ROUTE_TITLE]] = e
        else:
            if verbose:
                logger.info(
                    f"Created plan {plan_initializer.response_json[CircuitColumns.ID]} for "
                    f"{row[IntermediateColumns.ROUTE_TITLE]}."
                    f"\n{plan_initializer.response_json}"
                )
        finally:
            plan_df.loc[idx, IntermediateColumns.PLAN_ID] = (
                plan_initializer.plan_id
                if "plan_id" in vars(plan_initializer)
                else "plans/noID"
            )
            plan_df.loc[idx, CircuitColumns.WRITABLE] = (
                plan_initializer.writable if "writable" in vars(plan_initializer) else False
            )

    logger.info(f"Finished initializing plans. Initialized {idx + 1 - len(errors)} plans.")

    plan_df[IntermediateColumns.INITIALIZED] = True
    plan_df.loc[
        (plan_df[IntermediateColumns.ROUTE_TITLE].isin(errors.keys()))
        # TODO: Make not_writable a failure within class?
        | ~(plan_df[CircuitColumns.WRITABLE] == True),  # noqa: E712
        IntermediateColumns.INITIALIZED,
    ] = False

    if errors:
        logger.warning(f"Errors initializing plans:\n{errors}")

    not_writable = plan_df[~(plan_df[CircuitColumns.WRITABLE] == True)]  # noqa: E712
    if not not_writable.empty:
        logger.warning(f"Plan is not writable for the following routes:\n{not_writable}")

    return plan_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _build_plan_stops(
    stops_df: DataFrame[schema.Stops], plan_df: DataFrame[schema.PlansBuildStopsIn]
) -> dict[str, list[list[dict[str, dict[str, str] | list[str] | int | str]]]]:
    """Build stop arrays for each route.

    Args:
        stops_df: The long DataFrame with all the routes.
        plan_df: The DataFrame with the plan IDs and driver IDs for each sheet.

    Returns:
        For each plan, a list of stop arrays for batch stop uploads.
    """
    stops_df = _parse_addresses(stops_df=stops_df)
    plan_stops = {}
    for _, plan_row in plan_df.iterrows():
        plan_id = plan_row[IntermediateColumns.PLAN_ID]
        route_title = plan_row[IntermediateColumns.ROUTE_TITLE]
        route_stops = stops_df[stops_df[IntermediateColumns.SHEET_NAME] == route_title]
        plan_stops[plan_id] = _build_stop_array(
            route_stops=route_stops, driver_id=plan_row[CircuitColumns.ID]
        )

    for plan_id, all_stops in plan_stops.items():
        stop_arrays = []
        # Split all_stops into chunks of 100 stops.
        number_of_stops = len(all_stops)
        for i in range(0, number_of_stops, RateLimits.BATCH_STOP_IMPORT_MAX_STOPS):
            stop_arrays.append(
                all_stops[i : i + 100]  # noqa: E203
            )  # TODO: Add noqa E203 to shared, and remove throughout codebase.
        plan_stops[plan_id] = stop_arrays

    return plan_stops


# TODO: Why isn't this throwing when driver ID is invalid?
@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _get_all_drivers() -> schema.DriversGetAllDriversOut:
    """Get all drivers."""
    logger.info("Getting all drivers from Circuit ...")
    driver_pages = get_responses(url=CIRCUIT_DRIVERS_URL)
    logger.info("Finished getting drivers.")
    drivers_list = concat_response_pages(
        page_list=driver_pages, data_key=CircuitColumns.DRIVERS
    )
    drivers_df = pd.DataFrame(drivers_list)
    drivers_df = drivers_df.sort_values(by=CircuitColumns.NAME).reset_index(drop=True)

    return drivers_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _assign_drivers(
    drivers_df: DataFrame[schema.DriversAssignDriversIn],
    plan_df: DataFrame[schema.PlansAssignDriversIn],
) -> DataFrame[schema.PlansAssignDriversOut]:
    """Ask users to assign drivers to each route."""
    for idx, row in drivers_df.iterrows():
        print(
            f"{idx + 1}. {'Active' if row[CircuitColumns.ACTIVE] else 'Inactive'}: "
            f"{row[CircuitColumns.NAME]} {row[CircuitColumns.EMAIL]}"
        )

    print("\nUsing the driver numbers above, assign drivers to each route:")
    for route_title in plan_df[IntermediateColumns.ROUTE_TITLE]:
        plan_df = _assign_driver(
            route_title=route_title, drivers_df=drivers_df, plan_df=plan_df
        )

    for _, row in plan_df.iterrows():
        print(
            f"{row[IntermediateColumns.ROUTE_TITLE]}: "
            f"{row[IntermediateColumns.DRIVER_NAME]}, {row[CircuitColumns.EMAIL]}"
        )
    confirm = input("Confirm the drivers above? (y/n): ")
    # TODO: Check for y, n, and prompt again if neither.
    if confirm.lower() != "y":
        plan_df = _assign_drivers(drivers_df=drivers_df, plan_df=plan_df)

    return plan_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
# TODO: Decrease complexity. (Eemove noqa C901.)
def _assign_driver(  # noqa: C901
    route_title: str,
    drivers_df: DataFrame[schema.DriversAssignDriverIn],
    plan_df: DataFrame[schema.PlansAssignDriverIn],
) -> pd.DataFrame:
    """Ask user to assign driver to a route."""
    # TODO: Warn/raise if driver not active.
    best_guesses = pd.DataFrame()
    for name_part in route_title.split(" ")[1:]:
        if name_part not in ["&", "AND"] and len(name_part) > 1:
            best_guesses = pd.concat(
                [
                    best_guesses,
                    drivers_df[
                        drivers_df[CircuitColumns.NAME].str.contains(name_part, case=False)
                    ],
                ]
            )
    best_guesses = best_guesses.drop_duplicates().sort_values(by=CircuitColumns.NAME)

    print(f"\nRoute {route_title}:\nBest guesses:")
    for idx, driver in best_guesses.iterrows():
        print(
            f"{idx + 1}. {'Active' if driver[CircuitColumns.ACTIVE] else 'Inactive'}: "
            f"{driver[CircuitColumns.NAME]} {driver[CircuitColumns.EMAIL]}"
        )
    print("\n")

    assigned = False
    while not assigned:
        try:
            # TODO: Wrap this for test mock.
            # TODO: Add B907 to shared ignore list, and remove r"" throughout.
            # TODO: Add option to correct the previous.
            choice = input(
                f"Enter the number of the driver for '{route_title}'"  # noqa: B907
                "(ctl+c to start over):"
            )
        except ValueError:
            print("Invalid input. Please enter a number.")

        else:
            choice = choice if choice else "-1"
            try:
                choice = int(choice.strip()) - 1
                if choice < 0 or choice >= len(drivers_df):
                    raise errors.AssignmentOutOfRange
                driver = drivers_df.iloc[choice]
                if not driver[CircuitColumns.ACTIVE]:
                    raise errors.InactiveDriverAssignment
            except ValueError:
                print("Invalid input. Please enter a number.")
            except errors.AssignmentOutOfRange:
                print("Invalid input. Please enter a number associated with a driver.")
            except errors.InactiveDriverAssignment:
                print("Inactive driver selected. Select an active driver.")
            else:
                plan_df.loc[
                    plan_df[IntermediateColumns.ROUTE_TITLE] == route_title,
                    [
                        CircuitColumns.ID,
                        IntermediateColumns.DRIVER_NAME,
                        CircuitColumns.EMAIL,
                        CircuitColumns.ACTIVE,
                    ],
                ] = [
                    drivers_df.iloc[choice][CircuitColumns.ID],
                    drivers_df.iloc[choice][CircuitColumns.NAME],
                    drivers_df.iloc[choice][CircuitColumns.EMAIL],
                    drivers_df.iloc[choice][CircuitColumns.ACTIVE],
                ]
                assigned = True
                print(
                    f"\nAssigned {route_title} "
                    f"to {drivers_df.iloc[choice][CircuitColumns.NAME]}."
                )

    return plan_df


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


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _confirm_optimizations(
    plan_df: DataFrame[schema.PlansConfirmOptimizationsIn],
    optimizations: dict[str, str],
    verbose: bool,
) -> DataFrame[schema.PlansConfirmOptimizationsOut]:
    """Confirm all optimizations have finished."""
    logger.info("Confirming optimizations have finished ...")

    optimizations_finished: dict[str, bool | str] = {
        plan_id: False
        for plan_id in plan_df[plan_df[IntermediateColumns.OPTIMIZED] == True][  # noqa: E712
            IntermediateColumns.PLAN_ID
        ].to_list()
    }
    errors = {}

    while not all([val is True or val == "error" for val in optimizations_finished.values()]):
        unfinished = [
            plan_id
            for plan_id, finished in optimizations_finished.items()
            if not finished or finished != "error"
        ]
        for plan_id in unfinished:
            plan_title = plan_df[plan_df[IntermediateColumns.PLAN_ID] == plan_id][
                IntermediateColumns.ROUTE_TITLE
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
                errors[plan_title] = [e]

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
    # TODO: FutureWarning: Setting an item of incompatible dtype is deprecated and will raise
    # an error in a future version of pandas. Value 'nan' has dtype incompatible with bool,
    # please explicitly cast to a compatible dtype first.
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)
        plan_df.loc[
            plan_df[IntermediateColumns.ROUTE_TITLE].isin(errors.keys()),
            IntermediateColumns.OPTIMIZED,
        ] = None
    if errors:
        logger.warning(f"Errors checking optimizations:\n{errors}")

    return plan_df


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
