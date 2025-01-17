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

from bfb_delivery.lib.constants import COMBINED_ROUTES_COLUMNS, Columns, RateLimits
from bfb_delivery.lib.dispatch.utils import get_circuit_key
from bfb_delivery.lib.utils import get_friday


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

    # TODO: All we really need is a df of plan IDs and titles.
    plans = _get_plans(start_date=start_date)
    routes_df = _get_raw_routes_df(plans=plans)
    del plans

    # TODO: Validate single box count and single type.
    # TODO: Validate that route:driver_sheet_name is 1:1.
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
def _get_plans(start_date: str) -> list[dict[str, Any]]:
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
def _get_raw_routes_df(plans: list[dict[str, Any]]) -> pd.DataFrame:
    """Get the raw routes DataFrame from the plans."""
    # TODO: Add external ID for delivery day so we can filter stops by it in request?
    # After taking over upload.
    routes_dfs: list[pd.DataFrame] = []
    # TODO: Make Circuit columns constant?
    input_cols = [
        "route",
        "stopPosition",
        "recipient",
        "address",
        "notes",
        "orderInfo",
        "packageCount",
    ]
    for plan in plans:
        plan_id = plan.get("id", None)  # e.g., "plans/AqcSgl1s1MDonjzYBHM2"
        # https://developer.team.getcircuit.com/api#tag/Stops/operation/listStops
        base_url = f"https://api.getcircuit.com/public/v0.2b/{plan_id}/stops"
        wait_seconds = RateLimits.READ_SECONDS
        next_page_token = ""
        plan_stops_dfs: list[pd.DataFrame] = []

        while next_page_token is not None:
            url = base_url + str(next_page_token)
            stops_response = requests.get(url, auth=HTTPBasicAuth(get_circuit_key(), ""))
            if stops_response.status_code != 200:
                try:
                    response_json: dict = stops_response.json()
                except Exception as e:
                    response_json = {
                        "reason": stops_response.reason,
                        "additional_notes": "No-JSON response.",
                        "response to JSON exception:": str(e),
                    }
                err_msg = (
                    f"Got {stops_response.status_code} reponse when getting stops for "
                    f"{plan_id}: {response_json}"
                )
                if stops_response.status_code == 429:
                    wait_seconds = wait_seconds * 2
                    warn(
                        f"Doubling per-request wait time to {wait_seconds} seconds. {err_msg}"
                    )
                else:
                    raise ValueError(err_msg)
            else:
                stops = stops_response.json()
                next_page_token = stops.get("nextPageToken", None)

                stops_df = pd.DataFrame(stops["stops"])
                stops_df = stops_df[input_cols]
                plan_stops_dfs.append(stops_df)

            if next_page_token or stops_response.status_code == 429:
                next_page_token = f"?pageToken={next_page_token}"
                sleep(wait_seconds)

        plan_stops_df = pd.concat(plan_stops_dfs)
        plan_stops_df["driver_sheet_name"] = plan.get("title")  # e.g. "1.17 Jay C"
        routes_dfs.append(plan_stops_df)

    return pd.concat(routes_dfs)


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
            "stopPosition": Columns.STOP_NO,
            "notes": Columns.NOTES,
            "packageCount": Columns.ORDER_COUNT,
            "address": Columns.ADDRESS,
        },
        inplace=True,
    )
    routes_df["route"] = routes_df["route"].apply(lambda route_dict: route_dict.get("id"))
    # routes_df[Columns.STOP_NO] = routes_df[Columns.STOP_NO].astype(int) + 1
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
    # TODO: Ask if they want to get neighborhood from address instead of externalId.
    # Or, just impute from there when missing?
    routes_df[Columns.NEIGHBORHOOD] = routes_df["recipient"].apply(
        lambda recipient_dict: recipient_dict.get("externalId")
    )
    # TODO: Verify we want to do this.
    # TODO: Handle if neighborhood is missing from address, too. (Make function.)
    # TODO: Strip whitespace.
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

    # TODO: Warn and impute missing values.
    # Required (fail if missing): route, driver_sheet_name, stop_no, addressLineOne,
    # addressLineTwo. Give plan ID and instruct to download the routes from Circuit.
    # Optional (warn and impute if missing): name, phone, notes, order_count, box_type,
    # neighborhood
    # TODO: What to do with empty product types?
    routes_df[Columns.ADDRESS] = (
        routes_df["addressLineOne"] + ", " + routes_df["addressLineTwo"]
    )
    routes_df = routes_df[output_cols]
    routes_df.sort_values(by=["route", "driver_sheet_name", Columns.STOP_NO], inplace=True)

    return routes_df
