"""split_chunked_route CLI. See :doc:`split_chunked_route` for more information."""

from pathlib import Path

import click
from typeguard import typechecked

from bfb_delivery import combine_route_tables


# TODO: Can we set the defaults as constants to sync with public?
@click.command()
@click.option(
    "--input_paths", multiple=True, required=True, help="The paths to the driver route CSVs."
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default="",
    help=(
        "The directory to write the output workbook to. Empty string (default) saves to "
        "to the first input path's parent directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default="",
    help=(
        "The name of the output workbook. Empty string (default) will name the file "
        'combined_routes_{date}.xlsx".'
    ),
)
@typechecked
def main(input_paths: tuple[str, ...], output_dir: str, output_filename: str) -> Path:
    """Combines the driver route CSVs into a single workbook.

    This is used after optimizing and exporting the routes to individual CSVs.

    Returns:
        The path to the output workbook.
    """
    path = combine_route_tables(
        input_paths=list(input_paths), output_dir=output_dir, output_filename=output_filename
    )
    click.echo(f"Split workbook saved to: {path}")
    return path
