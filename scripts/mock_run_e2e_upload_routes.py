"""Mock run of the E2E upload routes."""

import json
from contextlib import nullcontext
from unittest.mock import patch

import click
import pandas as pd

from bfb_delivery.lib.dispatch.write_to_circuit import upload_split_chunked


@click.command()
@click.option(
    "--start-date",
    type=str,
    required=True,
    help="The start date for the routes to be uploaded, as 'YYYY-MM-DD'.",
)
@click.option("--verbose", is_flag=True, help="Print verbose output.")
@click.option("--mock-plan-creation", is_flag=True, help="Mock the plan creation process.")
@click.option(
    "--mock-upload-stop-array", is_flag=True, help="Mock the upload stop array process."
)
def main(
    start_date: str, verbose: bool, mock_plan_creation: bool, mock_upload_stop_array: bool
) -> None:
    """Run the main function."""
    mock_plans = pd.read_csv(".test_data/scratch/klub_plans.csv")
    plan_creation_context = (
        patch(
            "bfb_delivery.lib.dispatch.write_to_circuit._create_plans",
            return_value=mock_plans,
        )
        if mock_plan_creation
        else nullcontext()
    )

    with open(".test_data/scratch/uploaded_plan_stops.json", "r") as f:
        mock_uploaded_stops = json.load(f)
    upload_stop_array_context = (
        patch(
            "bfb_delivery.lib.dispatch.write_to_circuit._upload_stop_array",
            return_value=mock_uploaded_stops,
        )
        if mock_upload_stop_array
        else nullcontext()
    )

    # split_chunked_sheet_fp = ".test_data/split_chunked/single_split_chunked_1.xlsx"
    split_chunked_sheet_fp = ".test_data/split_chunked/test_driver.xlsx"
    with plan_creation_context, upload_stop_array_context:
        sheet_plan_df, uploaded_stops = upload_split_chunked(
            split_chunked_workbook_fp=split_chunked_sheet_fp,
            start_date=start_date,
            distribute=False,
            verbose=verbose,
        )
    breakpoint()
    print(sheet_plan_df)


if __name__ == "__main__":
    main()
