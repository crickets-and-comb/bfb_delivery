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
    COMBINED_ROUTES_COLUMNS,
    DEPOT_PLACE_ID,
    Columns,
    RateLimits,
)
from bfb_delivery.lib.dispatch.utils import get_circuit_key
from bfb_delivery.lib.schema import CircuitPlans, CircuitPlansFromDict, CircuitRoutesConcatOut
from bfb_delivery.lib.schema.utils import schema_error_handler
from bfb_delivery.lib.utils import get_friday

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# TODO: Add info/progess logging.
# TODO: Drop "All HHs" routes (is the single long route I think).
# Maybe add flag to get only them or not them. They can use this to get the
# initial route. Make get_route_files CLI, then.


@typechecked
def get_route_files(start_date: str, end_date: str, output_dir: str, all_HHs: bool) -> str:
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
            NOTE: True returns email column in CSV, for reuploading after splitting.

    Returns:
        The path to the route files.
    """
    start_date = start_date if start_date else get_friday(fmt="%Y%m%d")
    end_date = end_date if end_date else start_date
    if not output_dir:
        output_dir = os.getcwd() + "/routes_" + start_date

    plans_list = _get_raw_plans(start_date=start_date, end_date=end_date)
    # TODO: Filter to only plans with routes to make sure we don't get plans that aren't
    # routed. That would cause a validation problem when we check stops against routes.
    plans_df = _make_plans_df(plans_df=plans_list, all_HHs=all_HHs)
    del plans_list
    # TODO: Add external ID for delivery day so we can filter stops by it in request?
    # After taking over upload.
    plan_stops_list = _get_raw_stops_lists(plan_ids=plans_df["id"].tolist())
    # TODO: Filter to only stops with a route id to ensure we don't get stops with a plan and
    # no route. (I.e., not optimized and routed.)
    routes_df = _concat_routes_df(plan_stops_list=plan_stops_list, plans_df=plans_df)

    # TODO: Validate single box count and single type.
    # TODO: Validate that route:title is 1:1. (plan title is driver sheet name)
    # TODO: Validate that route title is same as plan title. (i.e., driver sheet name)

    routes_df = _transform_routes_df(routes_df=routes_df, include_email=all_HHs)
    _write_routes_dfs(routes_df=routes_df, output_dir=Path(output_dir), include_email=all_HHs)

    return output_dir


@typechecked
def _get_raw_plans(start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Call Circuit API to get the plans for the given date."""
    # TODO: Filter to upper date too? (Do they only do one day at a time?)
    # https://developer.team.getcircuit.com/api#tag/Plans/operation/listPlans
    url = (
        "https://api.getcircuit.com/public/v0.2b/plans"
        f"?filter.startsGte={start_date}"
        f"&filter.startsLte={end_date}"
    )
    logger.info(f"Getting route plans from {url} \n ...")
    plans = _get_responses(base_url=url)
    logger.info("Finished getting route plans.")
    plans_list = _concat_response_pages(page_list=plans, data_key="plans")
    return plans_list


# Using from_format config https://pandera.readthedocs.io/en/v0.22.1/
# data_format_conversion.html#data-format-conversion
# Not a fan of this as it obscures the pipeline steps and makes it harder to follow.
# Here, you pass in plans_df as a list of dictionaries, but you treat/type it as a dataframe.
# But, I want to use it once in a simple place to see how it works.
@pa.check_types(with_pydantic=True, lazy=True)
def _make_plans_df(
    plans_df: DataFrame[CircuitPlansFromDict], all_HHs: bool
) -> DataFrame[CircuitPlans]:
    """Make the plans DataFrame from the plans."""
    # plans_df = pd.DataFrame(plans_list)
    plans_df = plans_df[["id", "title"]]
    # TODO: We could do this in a few ways that are more robust.
    # 1. Filter by driver ID, but we'd need to exclude the staff that use their driver IDs
    # to do this, and what if they decided to drive one day?
    # 2. Pass an external ID to filter on.
    # 3. Create a dummy driver ID for the "All HHs" route.
    # 4. Pass title filter once we're confident in the title because we uploaded it
    # programmatically.
    # Worst to best in order.
    if all_HHs:
        plans_df = plans_df[plans_df["title"].str.contains(ALL_HHS_DRIVER)]
    else:
        plans_df = plans_df[~(plans_df["title"].str.contains(ALL_HHS_DRIVER))]

    return plans_df


@typechecked
def _get_raw_stops_lists(plan_ids: list[str]) -> list[dict[str, Any]]:
    """Get the raw stops list from Circuit."""
    plan_stops_list = []
    for plan_id in plan_ids:
        # https://developer.team.getcircuit.com/api#tag/Stops/operation/listStops
        url = f"https://api.getcircuit.com/public/v0.2b/{plan_id}/stops"
        logger.info(f"Getting route from {url} \n ...")
        stops_lists = _get_responses(base_url=url)
        logger.info("Finished getting route.")
        plan_stops_list += _concat_response_pages(page_list=stops_lists, data_key="stops")

    return plan_stops_list


@schema_error_handler
@pa.check_types(with_pydantic=True)
def _concat_routes_df(
    plan_stops_list: list[dict[str, Any]], plans_df: DataFrame[CircuitPlans]
) -> DataFrame[CircuitRoutesConcatOut]:
    """Concatenate the routes DataFrames from the plan stops lists."""
    routes_df = pd.DataFrame(plan_stops_list)
    # TODO: Make Circuit columns constant?
    routes_df = routes_df[
        [
            # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
            "plan",
            "route",
            # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
            "id",
            "stopPosition",
            "recipient",
            "address",
            "notes",
            "orderInfo",
            "packageCount",
        ]
    ]

    routes_df = routes_df.merge(
        plans_df.copy().rename(columns={"id": "plan_id"}),
        left_on="plan",
        right_on="plan_id",
        how="left",
        validate="m:1",
    )

    # TODO: Validate that all IDs and titles represented.

    return routes_df


@typechecked
def _transform_routes_df(routes_df: pd.DataFrame, include_email: bool) -> pd.DataFrame:
    """Transform the raw routes DataFrame."""
    # TODO: Make columns constant. (And/or use pandera.)
    output_cols = [
        "route",
        "driver_sheet_name",
        Columns.STOP_NO,
        Columns.NAME,
        Columns.ADDRESS,
        Columns.PHONE,
        Columns.NOTES,
        Columns.ORDER_COUNT,
        Columns.BOX_TYPE,
        Columns.NEIGHBORHOOD,
    ]
    if include_email:
        output_cols.append(Columns.EMAIL)

    routes_df.rename(
        columns={
            "title": "driver_sheet_name",  # Plan title is upload/download sheet name.
            "stopPosition": Columns.STOP_NO,
            "notes": Columns.NOTES,
            "packageCount": Columns.ORDER_COUNT,
            "address": Columns.ADDRESS,
        },
        inplace=True,
    )

    # Drop depot.
    routes_df["placeId"] = routes_df[Columns.ADDRESS].apply(
        lambda address_dict: address_dict.get("placeId")
    )
    routes_df = routes_df[routes_df["placeId"] != DEPOT_PLACE_ID]
    routes_df["route"] = routes_df["route"].apply(lambda route_dict: route_dict.get("id"))
    routes_df[Columns.NAME] = routes_df["recipient"].apply(
        lambda recipient_dict: recipient_dict.get("name")
    )
    routes_df["addressLineOne"] = routes_df[Columns.ADDRESS].apply(
        lambda address_dict: address_dict.get("addressLineOne")
    )
    routes_df["addressLineTwo"] = routes_df[Columns.ADDRESS].apply(
        lambda address_dict: address_dict.get("addressLineTwo")
    )
    routes_df[Columns.PHONE] = routes_df["recipient"].apply(
        lambda recipient_dict: recipient_dict.get("phone")
    )
    routes_df[Columns.BOX_TYPE] = routes_df["orderInfo"].apply(
        lambda order_info_dict: (
            order_info_dict["products"][0] if order_info_dict.get("products") else None
        )
    )
    routes_df[Columns.NEIGHBORHOOD] = routes_df["recipient"].apply(
        lambda recipient_dict: recipient_dict.get("externalId")
    )
    if include_email:
        routes_df[Columns.EMAIL] = routes_df["recipient"].apply(
            lambda recipient_dict: recipient_dict.get("email")
        )

    # TODO: Verify we want to warn/raise/impute.
    # TODO: Validate required not null: route, driver_sheet_name, stop_no, addressLineOne,
    # addressLineTwo, name, box_type.
    # Give plan ID and instruct to download the routes from Circuit.
    _warn_and_impute(routes_df=routes_df)
    routes_df[Columns.ADDRESS] = (
        routes_df["addressLineOne"] + ", " + routes_df["addressLineTwo"]
    )
    routes_df = routes_df[output_cols]
    routes_df.sort_values(by=["driver_sheet_name", Columns.STOP_NO], inplace=True)

    return routes_df


@typechecked
def _write_routes_dfs(routes_df: pd.DataFrame, output_dir: Path, include_email: bool) -> None:
    """Split and write the routes DataFrame to the output directory.

    Args:
        routes_df: The routes DataFrame to write.
        output_dir: The directory to save the routes to.
        include_email: Whether to include the email column in the output.
    """
    if output_dir.exists():
        logger.warning(f"Output directory exists {output_dir}. Overwriting.")
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True)
    output_columns = COMBINED_ROUTES_COLUMNS.copy()
    if include_email:
        output_columns.append(Columns.EMAIL)

    logger.info(f"Writing route CSVs to {output_dir.resolve()}")
    for route, route_df in routes_df.groupby("route"):
        driver_sheet_names = route_df["driver_sheet_name"].unique()
        if len(driver_sheet_names) > 1:
            raise ValueError(
                f"Route {route} has multiple driver sheet names: {driver_sheet_names}"
            )
        elif len(driver_sheet_names) < 1:
            raise ValueError(f"Route {route} has no driver sheet name.")
        driver_sheet_name = driver_sheet_names[0]
        route_df[output_columns].to_csv(output_dir / f"{driver_sheet_name}.csv", index=False)


@typechecked
def _get_responses(base_url: str) -> list[dict[str, Any]]:
    wait_seconds = RateLimits.READ_SECONDS
    next_page = ""
    responses = []

    while next_page is not None:
        url = base_url + str(next_page)
        response = requests.get(
            url,
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
            err_msg = f"Got {response.status_code} reponse for {url}: {response_dict}"

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
            token_prefix = "?" if "?" not in base_url else "&"
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
def _warn_and_impute(routes_df: pd.DataFrame) -> None:
    """Warn and impute missing values in the routes DataFrame."""
    missing_order_count = routes_df[Columns.ORDER_COUNT].isna()
    if missing_order_count.any():
        logger.warning(
            f"Missing order count for {missing_order_count.sum()} stops. Imputing 1 order."
        )
    routes_df[Columns.ORDER_COUNT] = routes_df[Columns.ORDER_COUNT].fillna(1)

    # TODO: Verify we want to do this. Ask, if we want to just overwrite the neighborhood.
    # TODO: Handle if neighborhood is missing from address, too. (Make function.)
    # TODO: Strip whitespace.
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
            else row[Columns.ADDRESS].get("address").split(",")[1]
        ),
        axis=1,
    )
