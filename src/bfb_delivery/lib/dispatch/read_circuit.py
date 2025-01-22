"""Read from Circuit."""

import logging
import os
import shutil
from pathlib import Path
from time import sleep
from typing import Any

import pandas as pd
import pandera as pa
import requests
from pandera.typing import DataFrame
from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from bfb_delivery.lib.constants import (
    ALL_HHS_DRIVER,
    CIRCUIT_DOWNLOAD_COLUMNS,
    DEPOT_PLACE_ID,
    CircuitColumns,
    Columns,
    IntermediateColumns,
    RateLimits,
)
from bfb_delivery.lib.dispatch.utils import get_circuit_key
from bfb_delivery.lib.schema import (
    CircuitPlansFromDict,
    CircuitPlansOut,
    CircuitPlansTransformIn,
    CircuitRoutesTransformInFromDict,
    CircuitRoutesTransformOut,
    CircuitRoutesWriteIn,
    CircuitRoutesWriteOut,
)
from bfb_delivery.lib.schema.utils import schema_error_handler
from bfb_delivery.lib.utils import get_friday

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@typechecked
def get_route_files(
    start_date: str, end_date: str, output_dir: str, all_HHs: bool, verbose: bool
) -> str:
    """Get the route files for the given date.

    Args:
        start_date: The start date to get the routes for, as "YYYYMMDD".
            Empty string uses the soonest Friday.
        end_date: The end date to get the routes for, as "YYYYMMDD".
            Empty string uses the start date.
        output_dir: The directory to save the routes to.
            Empty string saves to "routes_{date}" directory in present working directory.
            If the directory does not exist, it is created. If it exists, it is overwritten.
        all_HHs: Flag to get only the "All HHs" route.
            False gets all routes except "All HHs". True gets only the "All HHs" route.
        verbose: Flag to print verbose output.

    Returns:
        The path to the route files.

    Raises:
        ValueError: If no plans are found for the given date range.
        ValueError: If no plans with routes are found.
        ValueError: If no stops are found for the given plans.
        ValueError: If no routed stops are found for the given plans.
    """
    start_date = start_date if start_date else get_friday(fmt="%Y%m%d")
    end_date = end_date if end_date else start_date
    if not output_dir:
        output_dir = os.getcwd() + "/routes_" + start_date

    plans_list = _get_raw_plans(start_date=start_date, end_date=end_date, verbose=verbose)
    plans_df = _make_plans_df(plans_list=plans_list, all_HHs=all_HHs)
    # TODO: Add external ID for delivery day so we can filter stops by it in request?
    # After taking over upload.
    plan_stops_list = _get_raw_stops_lists(
        plan_ids=plans_df[CircuitColumns.ID].tolist(), verbose=verbose
    )
    routes_df = _transform_routes_df(plan_stops_list=plan_stops_list, plans_df=plans_df)
    _write_routes_dfs(routes_df=routes_df, output_dir=Path(output_dir))

    return output_dir


@typechecked
def _get_raw_plans(start_date: str, end_date: str, verbose: bool) -> list[dict[str, Any]]:
    """Call Circuit API to get the plans for the given date."""
    url = (
        "https://api.getcircuit.com/public/v0.2b/plans"
        f"?filter.startsGte={start_date}"
        f"&filter.startsLte={end_date}"
    )
    logger.info("Getting route plans from Circuit ...")
    if verbose:
        logger.info(f"Getting route plans from {url} ...")
    plans = _get_responses(url=url)
    logger.info("Finished getting route plans.")
    plans_list = _concat_response_pages(page_list=plans, data_key="plans")

    if not plans_list:
        raise ValueError(f"No plans found for {start_date} to {end_date}.")
    return plans_list


# Using from_format config https://pandera.readthedocs.io/en/v0.22.1/
# data_format_conversion.html#data-format-conversion
# Here, you pass in plans_df as a list of dictionaries, but you treat/type it as a dataframe.
# Not a huge fan as it obscures the pipeline steps and makes it a little harder to follow.
# But, it makes input validation simpler.
# NOTE: with_pydantic allows check of other params.
@pa.check_types(with_pydantic=True, lazy=True)
def _make_plans_df(
    plans_list: DataFrame[CircuitPlansFromDict], all_HHs: bool
) -> DataFrame[CircuitPlansOut]:
    """Make the plans DataFrame from the plans."""
    # What we'd do if not using from_format config:
    # plans_df = pd.DataFrame(plans_list)

    # TODO: We could drop All HHs in a few ways that are more robust.
    # 1. Won't work: Filter by driver ID, but we'd need to exclude the staff that use their
    # driver IDs to test things out, and what if they decided to drive one day?
    # (Make new driver ID if that ever happens.)
    # 2. Pass an external ID to filter on.
    # 3. Create a dummy driver ID for the "All HHs" route.
    # 4. Pass title filter once we're confident in the title because we uploaded it
    # programmatically.
    # Worst to best in order.
    # TODO: Set validation once we've settled on filter method.
    plan_count = len(plans_list)
    if all_HHs:
        logger.info(f'Filtering to only the "{ALL_HHS_DRIVER}" plan.')  # noqa: B907
        plans_df = plans_list[
            [
                ALL_HHS_DRIVER.upper() in title.upper()
                for title in plans_list[CircuitColumns.TITLE]
            ]
        ]
    else:
        logger.info(f'Filtering to all plans except "{ALL_HHS_DRIVER}".')  # noqa: B907
        plans_df = plans_list[
            [
                ALL_HHS_DRIVER.upper() not in title.upper()
                for title in plans_list[CircuitColumns.TITLE]
            ]
        ]
    dropped_count = plan_count - len(plans_df)
    if not all_HHs and dropped_count != 1:
        logger.warning(f"Dropped {dropped_count} plans.")
    elif dropped_count:
        logger.info(f"Dropped {dropped_count} plans.")
    else:
        logger.info("Dropped no plans.")

    plan_count = len(plans_df)
    if all_HHs and plan_count != 1:
        logger.warning(
            f'Got {plan_count} "{ALL_HHS_DRIVER}" plans. Expected 1.'  # noqa: B907
        )

    logger.info("Filtering out plans with no routes.")
    plan_count = len(plans_df)
    routed_plans_mask = [isinstance(val, list) and len(val) > 0 for val in plans_df["routes"]]
    plans_df = plans_df[routed_plans_mask]
    dropped_count = plan_count - len(plans_df)
    if dropped_count:
        logger.warning(f"Dropped {dropped_count} plans without routes.")
    else:
        logger.info("Dropped no plans.")

    plan_count = len(plans_df)
    if not plan_count:
        raise ValueError("No routes found for the given date range.")
    else:
        logger.info(f"Left with {plan_count} plans.")

    plans_df = plans_df[[CircuitColumns.ID, CircuitColumns.TITLE]]

    return plans_df


@typechecked
def _get_raw_stops_lists(plan_ids: list[str], verbose: bool) -> list[dict[str, Any]]:
    """Get the raw stops list from Circuit."""
    plan_stops_list = []
    logging.info("Getting stops from Circuit ...")
    for plan_id in plan_ids:
        # https://developer.team.getcircuit.com/api#tag/Stops/operation/listStops
        url = f"https://api.getcircuit.com/public/v0.2b/{plan_id}/stops"
        if verbose:
            logger.info(f"Getting stops from {url} ...")
        stops_lists = _get_responses(url=url)
        plan_stops_list += _concat_response_pages(
            page_list=stops_lists, data_key=CircuitColumns.STOPS
        )
    logger.info("Finished getting stops.")
    if not plan_stops_list:
        raise ValueError(f"No stops found for plans {plan_ids}.")

    return plan_stops_list


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _transform_routes_df(
    plan_stops_list: DataFrame[CircuitRoutesTransformInFromDict],
    plans_df: DataFrame[CircuitPlansTransformIn],
) -> DataFrame[CircuitRoutesTransformOut]:
    """Transform the raw routes DataFrame."""
    routes_df = _pare_routes_df(routes_df=plan_stops_list)
    del plan_stops_list

    routes_df = routes_df.merge(
        plans_df.copy().rename(columns={CircuitColumns.ID: "plan_id"}),
        left_on=CircuitColumns.PLAN,
        right_on="plan_id",
        how="left",
        validate="m:1",
    )

    routes_df.rename(
        columns={
            # Plan title is upload/download sheet name.
            CircuitColumns.TITLE: IntermediateColumns.DRIVER_SHEET_NAME,
            CircuitColumns.STOP_POSITION: Columns.STOP_NO,
            CircuitColumns.NOTES: Columns.NOTES,
            CircuitColumns.PACKAGE_COUNT: Columns.ORDER_COUNT,
            CircuitColumns.ADDRESS: Columns.ADDRESS,
        },
        inplace=True,
    )

    routes_df = _set_routes_df_values(routes_df=routes_df)

    routes_df.sort_values(
        by=[IntermediateColumns.DRIVER_SHEET_NAME, Columns.STOP_NO], inplace=True
    )

    return routes_df


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _write_routes_dfs(routes_df: DataFrame[CircuitRoutesWriteIn], output_dir: Path) -> None:
    """Split and write the routes DataFrame to the output directory.

    Args:
        routes_df: The routes DataFrame to write.
        output_dir: The directory to save the routes to.
        all_HHs: Flag to ensure email colum is in CSV for reuploading after splitting.
    """
    if output_dir.exists():
        logger.warning(f"Output directory exists {output_dir}. Overwriting.")
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True)

    logger.info(f"Writing route CSVs to {output_dir.resolve()}")
    for route, route_df in routes_df.groupby(CircuitColumns.ROUTE):
        driver_sheet_names = route_df[IntermediateColumns.DRIVER_SHEET_NAME].unique()
        if len(driver_sheet_names) > 1:
            raise ValueError(
                f"Route {route} has multiple driver sheet names: {driver_sheet_names}"
            )
        elif len(driver_sheet_names) < 1:
            raise ValueError(f"Route {route} has no driver sheet name.")

        route_df = route_df[CIRCUIT_DOWNLOAD_COLUMNS]
        driver_sheet_name = driver_sheet_names[0]
        fp = output_dir / f"{driver_sheet_name}.csv"
        _write_route_df(route_df=route_df, fp=fp)


@schema_error_handler
@pa.check_types(with_pydantic=True, lazy=True)
def _write_route_df(route_df: DataFrame[CircuitRoutesWriteOut], fp: Path) -> None:
    route_df.to_csv(fp, index=False)


# TODO: Pass params instead of forming URL first. ("params", not "json")
# (Would need to then grab params URL for next page?)
@typechecked
def _get_responses(url: str) -> list[dict[str, Any]]:
    wait_seconds = RateLimits.READ_SECONDS
    next_page = ""
    responses = []

    while next_page is not None:
        page_url = url + str(next_page)
        response = requests.get(
            page_url,
            auth=HTTPBasicAuth(get_circuit_key(), ""),
            timeout=RateLimits.READ_TIMEOUT_SECONDS,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_e:
            try:
                response_dict: dict = response.json()
            except Exception as e:
                response_dict = {
                    "reason": response.reason,
                    "additional_notes": "No-JSON response.",
                    "No-JSON response exception:": str(e),
                }
            err_msg = f"Got {response.status_code} reponse for {page_url}: {response_dict}"

            if response.status_code == 429:
                wait_seconds = wait_seconds * 2
                logger.warning(
                    f"{err_msg} . Doubling per-request wait time to {wait_seconds} seconds."
                )
            else:
                raise requests.exceptions.HTTPError(err_msg) from http_e

        else:
            stops = response.json()
            responses.append(stops)
            next_page = stops.get("nextPageToken", None)

        if next_page or response.status_code == 429:
            token_prefix = "?" if "?" not in url else "&"
            next_page = f"{token_prefix}pageToken={next_page}"
            sleep(wait_seconds)

    return responses


@typechecked
def _concat_response_pages(
    page_list: list[dict[str, Any]], data_key: str
) -> list[dict[str, Any]]:
    """Extract and concatenate the data lists from response pages."""
    data_list = []
    for page in page_list:
        data_list += page[data_key]

    return data_list


@typechecked
def _pare_routes_df(routes_df: pd.DataFrame) -> pd.DataFrame:
    routes_df = routes_df[
        [
            # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
            CircuitColumns.PLAN,
            CircuitColumns.ROUTE,
            # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
            CircuitColumns.ID,
            CircuitColumns.STOP_POSITION,
            CircuitColumns.RECIPIENT,
            CircuitColumns.ADDRESS,
            CircuitColumns.NOTES,
            CircuitColumns.ORDER_INFO,
            CircuitColumns.PACKAGE_COUNT,
        ]
    ]

    logger.info("Filtering out stops without routes.")
    stop_count = len(routes_df)
    routed_stops_mask = [
        isinstance(route_dict, dict) and route_dict.get(CircuitColumns.ID, "") != ""
        for route_dict in routes_df[CircuitColumns.ROUTE]
    ]
    routes_df = routes_df[routed_stops_mask]
    dropped_count = stop_count - len(routes_df)
    if dropped_count:
        logger.warning(f"Dropped {dropped_count} stops without routes.")
    else:
        logger.info("Dropped no stops.")

    logger.info("Filtering out depot stops.")
    stop_count = len(routes_df)
    routes_df[CircuitColumns.PLACE_ID] = routes_df[CircuitColumns.ADDRESS].apply(
        lambda address_dict: address_dict.get(CircuitColumns.PLACE_ID)
    )
    routes_df = routes_df[routes_df[CircuitColumns.PLACE_ID] != DEPOT_PLACE_ID]
    dropped_count = stop_count - len(routes_df)
    logger.info(f"Dropped {dropped_count} stops.")

    stop_count = len(routes_df)
    if not stop_count:
        raise ValueError("No routed stops found for the given plans.")
    else:
        logger.info(f"Left with {stop_count} routed stops.")

    return routes_df


@typechecked
def _set_routes_df_values(routes_df: pd.DataFrame) -> pd.DataFrame:
    routes_df[IntermediateColumns.ROUTE_TITLE] = routes_df[CircuitColumns.ROUTE].apply(
        lambda route_dict: route_dict.get(CircuitColumns.TITLE)
    )
    routes_df[CircuitColumns.ROUTE] = routes_df[CircuitColumns.ROUTE].apply(
        lambda route_dict: route_dict.get(CircuitColumns.ID)
    )
    routes_df[Columns.NAME] = routes_df[CircuitColumns.RECIPIENT].apply(
        lambda recipient_dict: recipient_dict.get(CircuitColumns.NAME)
    )
    routes_df[CircuitColumns.ADDRESS_LINE_1] = routes_df[Columns.ADDRESS].apply(
        lambda address_dict: address_dict.get(CircuitColumns.ADDRESS_LINE_1)
    )
    routes_df[CircuitColumns.ADDRESS_LINE_2] = routes_df[Columns.ADDRESS].apply(
        lambda address_dict: address_dict.get(CircuitColumns.ADDRESS_LINE_2)
    )
    routes_df[Columns.PHONE] = routes_df[CircuitColumns.RECIPIENT].apply(
        lambda recipient_dict: recipient_dict.get(CircuitColumns.PHONE)
    )
    routes_df[Columns.BOX_TYPE] = routes_df[CircuitColumns.ORDER_INFO].apply(
        lambda order_info_dict: (
            order_info_dict[CircuitColumns.PRODUCTS][0]
            if order_info_dict.get(CircuitColumns.PRODUCTS)
            else None
        )
    )
    routes_df[Columns.NEIGHBORHOOD] = routes_df[CircuitColumns.RECIPIENT].apply(
        lambda recipient_dict: recipient_dict.get(CircuitColumns.EXTERNAL_ID)
    )
    routes_df[Columns.EMAIL] = routes_df[CircuitColumns.RECIPIENT].apply(
        lambda recipient_dict: recipient_dict.get(CircuitColumns.EMAIL)
    )

    for title_col in [IntermediateColumns.DRIVER_SHEET_NAME, IntermediateColumns.ROUTE_TITLE]:
        routes_df[title_col] = routes_df[title_col].apply(
            lambda title: _clean_title(title=title)
        )

    # TODO: Verify we want to warn/raise/impute.
    # Give plan ID and instruct to download the routes from Circuit.
    _warn_and_impute(routes_df=routes_df)

    routes_df[Columns.ADDRESS] = (
        routes_df[CircuitColumns.ADDRESS_LINE_1]
        + ", "  # noqa: W503
        + routes_df[CircuitColumns.ADDRESS_LINE_2]  # noqa: W503
    )

    return routes_df


@typechecked
def _clean_title(title: str) -> str:
    """Clean the title."""
    if "/" in title:
        logger.warning('Title "{title}" contains "/". Replacing with ".".')  # noqa: B907
    return title.replace("/", ".")


@typechecked
def _warn_and_impute(routes_df: pd.DataFrame) -> None:
    """Warn and impute missing values in the routes DataFrame."""
    missing_order_count = routes_df[Columns.ORDER_COUNT].isna()
    if missing_order_count.any():
        logger.warning(
            f"Missing order count for {missing_order_count.sum()} stops. Imputing 1 order."
        )
    routes_df[Columns.ORDER_COUNT] = routes_df[Columns.ORDER_COUNT].fillna(1)

    # TODO: Verify we want to do this. Ask, if we want to just overwrite the neighborhood.
    missing_neighborhood = routes_df[Columns.NEIGHBORHOOD].isna()
    if missing_neighborhood.any():
        logger.warning(
            f"Missing neighborhood for {missing_neighborhood.sum()} stops."
            " Imputing best guesses from Circuit-supplied address."
        )
    routes_df[Columns.NEIGHBORHOOD] = routes_df[
        [Columns.NEIGHBORHOOD, Columns.ADDRESS]
    ].apply(
        lambda row: (
            row[Columns.NEIGHBORHOOD]
            if row[Columns.NEIGHBORHOOD]
            else row[Columns.ADDRESS].get(CircuitColumns.ADDRESS).split(",")[1]
        ),
        axis=1,
    )
