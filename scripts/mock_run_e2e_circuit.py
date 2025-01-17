"""Mock run of the end-to-end Circuit delivery script."""

import json
import os
import sys

import click

from bfb_delivery.lib.dispatch.read import _get_plans, _get_routes_by_plans
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../.test_data")))


@click.command()
@click.option(
    "--start_date",
    type=str,
    required=False,
    default="2025-01-14",
    help=(
        'The start date to use in the output workbook sheetnames as "YYYYMMDD".'
        "Empty string (default) uses the soonest Friday."
    ),
)
@click.option(
    "--use_mock_data",
    type=bool,
    required=False,
    default=True,
    help="Use mock data instead of querying the Circuit API, where possible.",
)
def main(start_date: str, use_mock_data: bool) -> None:
    """Mock run of the end-to-end Circuit integration."""
    if not use_mock_data:
        plans = _get_plans(start_date=start_date)
        routes_df = _get_routes_by_plans(plans=plans)
    else:
        with open(".test_data/sample_responses/plans.json") as f:
            plans = json.load(f)
            plans = plans["plans"]
        routes_df = pd.read_csv(".test_data/sample_responses/routes_df.csv")

    print(routes_df)
    breakpoint()


if __name__ == "__main__":
    main()
