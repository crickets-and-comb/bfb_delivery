"""Mock run of the end-to-end Circuit delivery script."""

import json
import os
import sys

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from bfb_delivery.lib.constants import Columns
from bfb_delivery.lib.dispatch.utils import get_circuit_key

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../.test_data")))

use_mock_data = True

api_key = get_circuit_key()
auth = HTTPBasicAuth(api_key, "")

start_date = "2025-01-14"
# plans_filter = [{"startsGte": start_date}]
# TODO: Handle error responses.
# TODO: Rate limit.
# TODO: Handle nextPageToken.
# TODO: Filter to upper date too? (Do they only do one day at a time?)
# https://developer.team.getcircuit.com/api#tag/Plans/operation/listPlans
if not use_mock_data:
    list_plans_response = requests.get(
        # "https://api.getcircuit.com/public/v0.2b/plans",
        f"https://api.getcircuit.com/public/v0.2b/plans?filter.startsGte={start_date}",
        # f"https://api.getcircuit.com/public/v0.2b/plans?filter[startsGte]={start_date}",
        auth=auth,
        # json=plans_filter,
    )
    plans = list_plans_response.json()

else:
    with open(".test_data/sample_responses/plans.json") as f:
        plans = json.load(f)

# TODO: Handle error responses.
# TODO: Rate limit.
# TODO: Handle nextPageToken.
# TODO: Add external ID for delivery day so we can filter stops by it in request?
# After taking over upload.
# TODO: Validate single box count and single type.
# TODO: Make Circuit columns constant.
# TODO: Validate that route:driver_sheet_name is 1:1.
# TODO: Validate that route title is same as plan title. (i.e., driver sheet name)
routes_dfs: list[pd.DataFrame] = []
input_cols = [
    "route",
    "stopPosition",
    "recipient",
    "address",
    "notes",
    "orderInfo",
    "packageCount",
]
for plan in plans.get("plans", {}):
    plan_id = plan["id"]  # e.g., "plans/AqcSgl1s1MDonjzYBHM2"
    # https://developer.team.getcircuit.com/api#tag/Stops/operation/listStops
    stops_response = requests.get(
        f"https://api.getcircuit.com/public/v0.2b/{plan_id}/stops", auth=auth
    )
    stops = stops_response.json()
    stops_df = pd.DataFrame(stops.get("stops"))
    stops_df = stops_df[input_cols]
    stops_df["driver_sheet_name"] = plan.get("title")  # e.g. "1.17 Jay C"
    routes_dfs.append(stops_df)

routes_df = pd.concat(routes_dfs)
breakpoint()
# del routes_dfs
# del plans
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
routes_df[Columns.NEIGHBORHOOD] = routes_df[Columns.NEIGHBORHOOD, Columns.ADDRESS].apply(
    lambda row: row[0] if row[0] else row[1].get("address").split(",")[1]
)

# TODO: Warn and impute missing values.
# Required (fail if missing): route, driver_sheet_name, stop_no, addressLineOne,
# addressLineTwo. Give plan ID and instruct to download the routes from Circuit.
# Optional (warn and impute if missing): name, phone, notes, order_count, box_type,
# neighborhood
# TODO: What to do with empty product types?
routes_df[Columns.ADDRESS] = routes_df["addressLineOne"] + ", " + routes_df["addressLineTwo"]
routes_df = routes_df[output_cols]
breakpoint()
