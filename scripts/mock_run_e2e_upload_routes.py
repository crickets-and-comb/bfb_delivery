"""Mock run of the E2E upload routes."""

import click

from bfb_delivery.lib.dispatch.write_to_circuit import upload_split_chunked


@click.command()
@click.option(
    "--ignore-inactive-drivers",
    is_flag=True,
    help="Ignore inactive drivers when uploading routes.",
)
def main(ignore_inactive_drivers: bool) -> None:
    """Run the main function."""
    # split_chunked_sheet_fp = ".test_data/split_chunked/single_split_chunked_1.xlsx.xlsx"
    split_chunked_sheet_fp = ".test_data/split_chunked/test_driver.xlsx"
    sheet_plan_df = upload_split_chunked(
        split_chunked_sheet_fp,
        "1888-12-23",
        distribute=False,
        ignore_inactive_drivers=ignore_inactive_drivers,
    )
    breakpoint()
    print(sheet_plan_df)


if __name__ == "__main__":
    main()
