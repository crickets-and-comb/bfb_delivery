"""Mock run of the E2E upload routes."""

import click

from bfb_delivery.lib.dispatch.write_to_circuit import upload_split_chunked


@click.command()
@click.option(
    "--start-date",
    type=str,
    required=True,
    help="The start date for the routes to be uploaded, as 'YYYY-MM-DD'.",
)
@click.option("--verbose", is_flag=True, help="Print verbose output.")
def main(start_date: str, verbose: bool) -> None:
    """Run the main function."""
    # split_chunked_sheet_fp = ".test_data/split_chunked/single_split_chunked_1.xlsx"
    split_chunked_sheet_fp = ".test_data/split_chunked/test_driver.xlsx"
    sheet_plan_df = upload_split_chunked(
        split_chunked_workbook_fp=split_chunked_sheet_fp,
        start_date=start_date,
        distribute=False,
        verbose=verbose,
    )
    breakpoint()
    print(sheet_plan_df)


if __name__ == "__main__":
    main()
