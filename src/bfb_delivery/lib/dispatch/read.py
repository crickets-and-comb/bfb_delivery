"""Read from Circuit."""

import os
import shutil
from pathlib import Path
from time import sleep
from typing import Any
from warnings import warn

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from bfb_delivery.lib.constants import (
    COMBINED_ROUTES_COLUMNS,
    DEPOT_PLACE_ID,
    Columns,
    RateLimits,
)
from bfb_delivery.lib.dispatch.utils import get_circuit_key
from bfb_delivery.lib.utils import get_friday

# TODO: Reorder functions.


@typechecked
def get_route_files(start_date: str, output_dir: str) -> str:
    """Get the route files for the given date.

    Args:
        start_date: The start date to get the routes for, as "YYYYMMDD".
            Empty string uses the soonest Friday.
        output_dir: The directory to save the routes to.
            Empty string saves to "routes_{date}" directory in present working directory.
            If the directory does not exist, it is created. If it exists, it is overwritten.

    Returns:
        The path to the route files.
    """
    start_date = start_date if start_date else get_friday(fmt="%Y%m%d")
    if not output_dir:
        output_dir = os.getcwd() + "/routes_" + start_date

    plans = _get_raw_plans(start_date=start_date)
    plans_df = _make_plans_df(plans=plans)
    del plans
    # TODO: Add external ID for delivery day so we can filter stops by it in request?
    # After taking over upload.
    plan_stops_lists = _get_raw_stops_lists(plan_ids=plans_df["id"].tolist())
    routes_df = _concat_routes_df(plan_stops_lists=plan_stops_lists, plans_df=plans_df)

    # TODO: Validate single box count and single type.
    # TODO: Validate that route:title is 1:1. (plan title is driver sheet name)
    # TODO: Validate that route title is same as plan title. (i.e., driver sheet name)

    routes_df = _transform_routes_df(routes_df=routes_df)
    _write_routes_dfs(routes_df=routes_df, output_dir=Path(output_dir))

    return output_dir


@typechecked
def _write_routes_dfs(routes_df: pd.DataFrame, output_dir: Path) -> None:
    """Split and write the routes DataFrame to the output directory.

    Args:
        routes_df: The routes DataFrame to write.
        output_dir: The directory to save the routes to.
    """
    if output_dir.exists():
        warn(f"Output directory exists {output_dir}. Overwriting.")
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True)

    for route, route_df in routes_df.groupby("route"):
        driver_sheet_names = route_df["driver_sheet_name"].unique()
        if len(driver_sheet_names) > 1:
            raise ValueError(
                f"Route {route} has multiple driver sheet names: {driver_sheet_names}"
            )
        elif len(driver_sheet_names) < 1:
            raise ValueError(f"Route {route} has no driver sheet name.")
        driver_sheet_name = driver_sheet_names[0]
        route_df[COMBINED_ROUTES_COLUMNS].to_csv(
            output_dir / f"{driver_sheet_name}.csv", index=False
        )


@typechecked
def _get_raw_plans(start_date: str) -> list[dict[str, Any]]:
    """Call Circuit API to get the plans for the given date."""
    # TODO: Handle error responses.
    # TODO: Rate limit.
    # TODO: Handle nextPageToken.
    # TODO: Filter to upper date too? (Do they only do one day at a time?)
    # https://developer.team.getcircuit.com/api#tag/Plans/operation/listPlans
    list_plans_response = requests.get(
        f"https://api.getcircuit.com/public/v0.2b/plans?filter.startsGte={start_date}",
        auth=HTTPBasicAuth(get_circuit_key(), ""),
    )
    plans = list_plans_response.json()

    return plans.get("plans", {})


@typechecked
def _make_plans_df(plans: list[dict[str, Any]]) -> pd.DataFrame:
    """Make the plans DataFrame from the plans."""
    plans_df = pd.DataFrame(plans)
    plans_df = plans_df[["id", "title"]]

    if plans_df.isna().any().any():
        raise ValueError("Plan ID or title is missing.")
    duplicates = plans_df[plans_df.duplicated(subset="title", keep=False)]
    if not duplicates.empty:
        raise ValueError(f"Duplicate plan id:titles:\n{duplicates}")
    if plans_df["id"].nunique() != plans_df["title"].nunique():
        raise ValueError("Plan ID and title are not 1:1.")

    return plans_df


@typechecked
def _get_raw_stops_lists(plan_ids: list[str]) -> list[dict[str, Any]]:
    """Get the raw stops list from Circuit."""
    plan_stops_lists = []
    for plan_id in plan_ids:
        # https://developer.team.getcircuit.com/api#tag/Stops/operation/listStops
        base_url = f"https://api.getcircuit.com/public/v0.2b/{plan_id}/stops"
        stops_lists = _get_responses(base_url=base_url)
        # TODO: Why are we appending? Just add?
        plan_stops_lists += stops_lists

    return plan_stops_lists


@typechecked
def _get_responses(base_url: str) -> list[dict[str, Any]]:
    wait_seconds = RateLimits.READ_SECONDS
    next_page_token = ""
    responses = []

    while next_page_token is not None:
        url = base_url + str(next_page_token)
        response = requests.get(url, auth=HTTPBasicAuth(get_circuit_key(), ""))
        if response.status_code != 200:
            try:
                response_dict: dict = response.json()
            except Exception as e:
                response_dict = {
                    "reason": response.reason,
                    "additional_notes": "No-JSON response.",
                    "response to JSON exception:": str(e),
                }
            err_msg = f"Got {response.status_code} reponse for {url}: {response_dict}"
            if response.status_code == 429:
                wait_seconds = wait_seconds * 2
                warn(f"Doubling per-request wait time to {wait_seconds} seconds. {err_msg}")
            else:
                raise ValueError(err_msg)
        else:
            stops = response.json()
            # TODO: Why are we appending?
            # Just drop token, select data lists and concat?
            # [{'nextPageToken': 'cm91dGVzL2xJVFRuUXN4WWZmcUpRRHhJcHpyL3N0b3BzL3hGYk1nN0RIcm1GOFhvd0RkdlNn', # noqa: B950, E501
            #   'stops': [{'activity': 'delivery',
            responses.append(stops)
            next_page_token = stops.get("nextPageToken", None)

        if next_page_token or response.status_code == 429:
            next_page_token = f"?pageToken={next_page_token}"
            sleep(wait_seconds)

    return responses


@typechecked
def _concat_routes_df(
    plan_stops_lists: list[dict[str, Any]], plans_df: pd.DataFrame
) -> pd.DataFrame:
    """Concatenate the routes DataFrames from the plan stops lists."""
    # TODO: Make Circuit columns constant?
    input_cols = [
        "plan",  # plan id, e.g. "plans/0IWNayD8NEkvD5fQe2SQ"
        "route",
        "id",  # stop id, e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b"
        "stopPosition",
        "recipient",
        "address",
        "notes",
        "orderInfo",
        "packageCount",
    ]
    routes_df = pd.concat(
        [pd.DataFrame(stops.get("stops", []))[input_cols] for stops in plan_stops_lists]
    )
    # TODO: Validate that no null ID.
    routes_df = routes_df.merge(
        plans_df, left_on="plan", right_on="id", how="left", validate="m:1"
    )
    # TODO: Validate that no null title.
    # TODO: Validate that all IDs and titles represented.
    # TODO: Validate that stop IDs are unique.

    return routes_df


@typechecked
def _transform_routes_df(routes_df: pd.DataFrame) -> pd.DataFrame:
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
def _warn_and_impute(routes_df: pd.DataFrame) -> None:
    """Warn and impute missing values in the routes DataFrame."""
    missing_order_count = routes_df[Columns.ORDER_COUNT].isna()
    if missing_order_count.any():
        warn(f"Missing order count for {missing_order_count.sum()} stops. Imputing 1.")
    routes_df[Columns.ORDER_COUNT] = routes_df[Columns.ORDER_COUNT].fillna(1)

    # TODO: Verify we want to do this. Ask, if we want to just overwrite the neighborhood.
    # TODO: Handle if neighborhood is missing from address, too. (Make function.)
    # TODO: Strip whitespace.
    missing_neighborhood = routes_df[Columns.NEIGHBORHOOD].isna()
    if missing_neighborhood.any():
        warn(
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
