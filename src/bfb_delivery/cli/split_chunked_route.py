"""This is just an example."""

from pathlib import Path

import click
from typeguard import typechecked

from bfb_delivery.api import public


# TODO: Can we set the defaults as constants to sync with public?
@click.command()
@click.option(
    "--sheet_path",
    type=str,
    required=True,
    help="Path to the chunked route sheet that this function reads in and splits up.",
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default="",
    help=(
        "Directory to save the output workbook. Empty string (default) saves to "
        "the input `sheet_path` directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default="",
    help=(
        "Name of the output workbook. Empty string (default) sets filename to "
        '"split_workbook_{date}_{i of n_books}.xlsx".'
    ),
)
@click.option(
    "--n_books",
    type=int,
    required=False,
    default=4,
    help="Number of workbooks to split into. Default is 4.",
)
@typechecked
def main(sheet_path: str, output_dir: str, output_filename: str, n_books: int) -> list[Path]:
    """Split route sheet into n workbooks with sheets by driver.

    Sheets by driver allows splitting routes by driver on Circuit upload.
    Multiple workbooks allows team to split the uploads among members, so one person
    doesn't have to upload all routes.
    This process follows the "chunking" process in the route generation, where routes
    are split into smaller "chunks" by driver (i.e., each stop is labeled with a driver).

    Reads a route spreadsheet at `sheet_path`.
    Writes `n_books` Excel workbooks with each sheet containing the stops for a single driver.
    Writes adjacent to the original workbook.

    Returns:
        Paths to the split chunked route workbooks.
    """
    paths = public.split_chunked_route(
        sheet_path=sheet_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
    )
    click.echo("Split workbook(s) saved to:")
    for path in paths:
        click.echo(path)
    return paths
