"""Mock run of the end-to-end Circuit delivery script.

Use this to test on real data. in your .test_data dir.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Final

import click

from bfb_delivery.lib.constants import CircuitColumns

# from bfb_delivery import create_manifests_from_circuit
from bfb_delivery.lib.dispatch.read_circuit import (
    _concat_routes_df,
    _get_raw_plans,
    _get_raw_stops_lists,
    _make_plans_df,
    _transform_routes_df,
    _write_routes_dfs,
)
from bfb_delivery.lib.formatting import sheet_shaping
from bfb_delivery.lib.utils import get_friday

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../.test_data")))

OUTPUT_DIRS: Final[dict[str, str]] = {
    "CIRCUIT_TABLES_DIR": ".test_data/circuit_tables",
    "MANIFESTS_DIR": ".test_data/manifests",
}


@click.command()
@click.option(
    "--start_date",
    type=str,
    required=False,
    # NOTE: Need to update eventually when data gets purged from Circuit.
    default="2025-01-17",
    help=(
        'The start date to use in the output workbook sheetnames as "YYYYMMDD". '
        "Empty string (default) uses the soonest Friday. Range is inclusive."
    ),
)
@click.option(
    "--end_date",
    type=str,
    required=False,
    default="",
    help=(
        'The end date to use in the output workbook sheetnames as "YYYYMMDD".'
        "Empty string (default) uses the start date. Range is inclusive."
    ),
)
@click.option(
    "--all_hhs",
    type=bool,
    required=False,
    default=False,
    help=(
        'Flag to get only the "All HHs" route.'
        'False gets all routes except "All HHs". True gets only the "All HHs" route.'
        "NOTE: True returns email column in CSV, for reuploading after splitting."
    ),
)
@click.option(
    "--mock_raw_plans",
    type=bool,
    required=False,
    default=True,
    help=(
        "Use mock plan data instead of querying the Circuit API. "
        "Only relevant if not using the public API, which will query Circuit."
    ),
)
@click.option(
    "--mock_raw_routes",
    type=bool,
    required=False,
    default=True,
    help=(
        "Use mock routes data instead of querying the Circuit API. "
        "Only relevant if not using the public API, which will query Circuit."
    ),
)
@click.option(
    "--use_public",
    type=bool,
    required=False,
    default=False,
    help=(
        "Use the public API instead of walking through internal helpers. "
        "Does not mock data; overrides mock_raw_*, always queries the Circuit API."
    ),
)
def main(  # noqa: C901
    start_date: str,
    end_date: str,
    all_hhs: bool,
    mock_raw_plans: bool,
    mock_raw_routes: bool,
    use_public: bool,
) -> None:
    """Mock run of the end-to-end Circuit integration."""
    for output_dir_key in OUTPUT_DIRS.keys():
        if output_dir_key != "CIRCUIT_TABLES_DIR":  # Func should delete it.
            this_output_dir = OUTPUT_DIRS[output_dir_key]
            shutil.rmtree(this_output_dir, ignore_errors=True)
            Path(this_output_dir).mkdir(parents=True)

    if use_public:
        # final_manifest_path = create_manifests_from_circuit(
        #     start_date=start_date,
        #     end_date=end_date,
        #     all_HHs=all_hhs,
        #     output_dir=OUTPUT_DIRS["MANIFESTS_DIR"],
        #     circuit_output_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
        # )
        # print(f"final_manifest_path: {final_manifest_path}")
        args_list = [
            "--output_dir",
            OUTPUT_DIRS["MANIFESTS_DIR"],
            "--circuit_output_dir",
            OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
        ]
        if start_date:
            args_list += ["--start_date", start_date]
        if end_date:
            args_list += ["--end_date", end_date]
        if all_hhs:
            args_list += ["--all_hhs", str(all_hhs).lower()]
        result = subprocess.run(["create_manifests_from_circuit"] + args_list)
        print(f"result: {result}")

    else:
        # BEGIN: get_route_files-ish
        if not mock_raw_plans:
            start_date = start_date if start_date else get_friday(fmt="%Y%m%d")
            end_date = end_date if end_date else start_date

            plans_list = _get_raw_plans(start_date=start_date, end_date=end_date)
            # with open(".test_data/sample_responses/plans_list.json", "w") as f:
            #     json.dump(plans_list, f, indent=4)

        else:
            with open(".test_data/sample_responses/plans_list.json") as f:
                plans_list = json.load(f)

        plans_df = _make_plans_df(plans_df=plans_list, all_HHs=all_hhs)
        # if all_hhs:
        #     plans_df.to_csv(".test_data/sample_responses/plans_df_all_hhs.csv", index=False)
        # else:
        #     plans_df.to_csv(".test_data/sample_responses/plans_df.csv", index=False)

        if not mock_raw_routes:
            plan_stops_list = _get_raw_stops_lists(
                plan_ids=plans_df[CircuitColumns.ID].to_list()
            )
            # if all_hhs:
            #     with open(
            #         ".test_data/sample_responses/plan_stops_list_all_hhs.json", "w"
            #     ) as f:
            #         json.dump(plan_stops_list, f, indent=4)
            # else:
            #     with open(".test_data/sample_responses/plan_stops_list.json", "w") as f:
            #         json.dump(plan_stops_list, f, indent=4)

        else:
            if all_hhs:
                with open(".test_data/sample_responses/plan_stops_list_all_hhs.json") as f:
                    plan_stops_list = json.load(f)
            else:
                with open(".test_data/sample_responses/plan_stops_list.json") as f:
                    plan_stops_list = json.load(f)

        routes_df = _concat_routes_df(plan_stops_list=plan_stops_list)
        # if all_hhs:
        #     routes_df.to_csv(".test_data/sample_responses/routes_df_raw_all_hhs.csv", index=False) # noqa: E501
        # else:
        #     routes_df.to_pickle(".test_data/sample_responses/routes_df_raw.pkl")

        routes_df = _transform_routes_df(routes_df=routes_df, plans_df=plans_df)
        # if all_hhs:
        #     routes_df.to_csv(
        #         ".test_data/sample_responses/routes_df_transformed_all_hhs.csv", index=False
        #     )  # noqa: E501
        # else:
        #     routes_df.to_csv(
        #         ".test_data/sample_responses/routes_df_transformed.csv", index=False
        #     )  # noqa: E501
        _write_routes_dfs(
            routes_df=routes_df, output_dir=Path(OUTPUT_DIRS["CIRCUIT_TABLES_DIR"])
        )
        # END: get_route_files

        formatted_manifest_path = sheet_shaping.create_manifests(
            input_dir=Path(OUTPUT_DIRS["CIRCUIT_TABLES_DIR"]),
            output_dir=Path(OUTPUT_DIRS["MANIFESTS_DIR"]),
            output_filename="",
            extra_notes_file="",
        )
        print(f"formatted_manifest_path: {formatted_manifest_path}")


if __name__ == "__main__":
    main()
