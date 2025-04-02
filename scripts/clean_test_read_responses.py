"""Sanitize sample responses from the test data to be used as fixtures for unit tests."""

import json
from pathlib import Path

import phonenumbers

from bfb_delivery.lib.constants import ALL_HHS_DRIVER

LEAVE_ALONE = [ALL_HHS_DRIVER, "Warehouse"]

driver_name_dict = {}
with open(Path(".test_data/sample_responses/plan_responses_dirty.json"), "r") as rf, open(
    Path("tests/unit/fixtures/plan_responses.json"), "w"
) as wf:
    plan_dicts = json.load(rf)

    cleaned_plans_list = []
    for i, plan_dict in enumerate(plan_dicts):
        cleaned_plan_dict = {}
        plan_dict["nextPageToken"] = plan_dict["nextPageToken"]

        cleaned_plan_dict["plans"] = []
        for j, plan in enumerate(plan_dict["plans"]):
            driver_name = " ".join(plan["title"].split(" ")[1:]).split("#")[0]
            dummy_driver_name = (
                f"Dummy Driver Name {i}-{j} "
                if driver_name not in LEAVE_ALONE
                else driver_name
            )
            driver_name_dict[driver_name] = dummy_driver_name

            cleaned_plan_dict["plans"].append(
                {
                    "id": plan["id"],
                    "title": plan["title"].replace(driver_name, dummy_driver_name),
                    "starts": plan["starts"],
                    "routes": plan["routes"],
                    "drivers": [{"id": driver["id"]} for driver in plan["drivers"]],
                }
            )

        cleaned_plans_list.append(cleaned_plan_dict)

    json.dump(cleaned_plans_list, wf, indent=4)


for all_hhs in ["_all_hhs", ""]:
    with open(
        Path(f".test_data/sample_responses/stops_responses_dirty{all_hhs}.json"), "r"
    ) as rf, open(Path(f"tests/unit/fixtures/stops_responses{all_hhs}.json"), "w") as wf:
        stops_lists = json.load(rf)

        cleaned_stops_lists = []
        for stops_list in stops_lists:
            cleaned_stops_list = []
            for i, stop_dict in enumerate(stops_list):
                cleaned_stop_dict = {}
                cleaned_stop_dict["nextPageToken"] = stop_dict["nextPageToken"]

                cleaned_stop_dict["stops"] = []
                for j, stop in enumerate(stop_dict["stops"]):
                    for driver_name, dummy_driver_name in driver_name_dict.items():
                        stop["route"]["title"] = stop["route"]["title"].replace(
                            driver_name, dummy_driver_name
                        )

                    cleaned_stop_dict["stops"].append(
                        {
                            "id": stop["id"],
                            "address": {
                                "address": (
                                    f"stop-{i}-{j} address, Dummy Imputed Neighborhood"
                                ),  # noqa: E501
                                "addressLineOne": f"stop-{i}-{j} addressLineOne",
                                "addressLineTwo": f"stop-{i}-{j} addressLineTwo",
                                "placeId": stop["address"]["placeId"],
                            },
                            "notes": stop["notes"],
                            "packageCount": stop["packageCount"],
                            "stopPosition": stop["stopPosition"],
                            "orderInfo": {"products": stop["orderInfo"]["products"]},
                            "recipient": {
                                "name": f"stop-{i}-{j} recipient",
                                "phone": (
                                    phonenumbers.example_number(
                                        region_code="US"
                                    ).national_number
                                ),
                            },
                            "plan": stop["plan"],
                            "route": {
                                "id": stop["route"]["id"],
                                "title": stop["route"]["title"],
                                "stopCount": stop["route"]["stopCount"],
                                "driver": stop["route"]["driver"],
                                "plan": stop["route"]["plan"],
                            },
                        }
                    )

                cleaned_stops_list.append(cleaned_stop_dict)

            cleaned_stops_lists.append(cleaned_stops_list)

        json.dump(cleaned_stops_lists, wf, indent=4)
