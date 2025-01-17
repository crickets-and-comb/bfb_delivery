"""Mock run of the end-to-end Circuit delivery script."""

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Final
import pickle

import click
import pandas as pd

from bfb_delivery import create_manifests_from_circuit
from bfb_delivery.lib.dispatch.read import (
    _get_plans,
    _get_raw_routes_df,
    _transform_routes_df,
    _write_routes_dfs,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../.test_data")))

OUTPUT_DIRS: Final[dict[str, str]] = {
    "CIRCUIT_TABLES_DIR": ".test_data/circuit_tables",
    "MANIFESTS_DIR": ".test_data/manifests",
    "EXTRA_NOTES_DIR": ".test_data/extra_notes",
}


@click.command()
@click.option(
    "--start_date",
    type=str,
    required=False,
    default="2025-01-14",
    help=(
        'The start date to use in the output workbook sheetnames as "YYYYMMDD". '
        "Empty string (default) uses the soonest Friday."
    ),
)
@click.option(
    "--use_mock_data",
    type=bool,
    required=False,
    default=True,
    help=(
        "Use mock data instead of querying the Circuit API, where possible. "
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
        "Does not mock data; overrides use_mock_data and always queries the Circuit API."
    ),
)
def main(start_date: str, use_mock_data: bool, use_public: bool) -> None:
    """Mock run of the end-to-end Circuit integration."""
    for output_dir_key in OUTPUT_DIRS.keys():
        if output_dir_key != "CIRCUIT_TABLES_DIR":
            this_output_dir = OUTPUT_DIRS[output_dir_key]
            shutil.rmtree(this_output_dir, ignore_errors=True)
            Path(this_output_dir).mkdir(parents=True)

    if use_public:
        final_manifest_path = create_manifests_from_circuit(
            start_date=start_date,
            output_dir=OUTPUT_DIRS["MANIFESTS_DIR"],
            circuit_output_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
            extra_notes_file=OUTPUT_DIRS["EXTRA_NOTES_DIR"],
        )
        print(final_manifest_path)
        breakpoint()
    else:
        if not use_mock_data:
            plans = _get_plans(start_date=start_date)
            routes_df = _get_raw_routes_df(plans=plans)
        else:
            with open(".test_data/sample_responses/plans.json") as f:
                plans = json.load(f)
                plans = plans["plans"]
            routes_df: pd.DataFrame
            with open(".test_data/sample_responses/routes_df.pkl", "rb") as f:
                routes_df = pickle.load(f)

        routes_df = _transform_routes_df(routes_df=routes_df)
        _write_routes_dfs(
            routes_df=routes_df, output_dir=Path(OUTPUT_DIRS["CIRCUIT_TABLES_DIR"])
        )

        print(routes_df)
        breakpoint()


if __name__ == "__main__":
    main()
