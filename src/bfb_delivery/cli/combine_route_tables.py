"""combine_route_tables CLI. See :doc:`combine_route_tables` for more information."""

from pathlib import Path

import click
from typeguard import typechecked

from bfb_delivery import combine_route_tables


# TODO: Can we set the defaults as constants to sync with public?
# TODO: They may want to just pass a text file of the paths instead of multiple args.
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
    """See public docstring: :py:func:`bfb_delivery.api.public.combine_route_tables`."""
    path = combine_route_tables(
        input_paths=list(input_paths), output_dir=output_dir, output_filename=output_filename
    )
    click.echo(f"Combined workbook saved to: {path}")
    return path
