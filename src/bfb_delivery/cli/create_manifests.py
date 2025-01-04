"""combine_route_tables CLI. See :doc:`create_manifests` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import create_manifests
from bfb_delivery.lib.constants import Defaults


@click.command()
@click.option(
    "--input_dir",
    type=str,
    required=True,
    help="The directory containing the driver route CSVs.",
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["output_dir"],
    help=(
        "The directory to write the output workbook to. Empty string (default) saves "
        "to the first input path's parent directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["output_filename"],
    help=(
        "The name of the output workbook. Empty string (default) will name the file "
        '"combined_routes_{date}.xlsx".'
    ),
)
@click.option(
    "--date",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["date"],
    help=(
        "The date to use in driver manifests. Empty string (default) will use today's date "
        "as {MM.DD}'"
    ),
)
@typechecked
def main(input_dir: str, output_dir: str, output_filename: str, date: str) -> str:
    """See public docstring: :py:func:`bfb_delivery.api.public.combine_route_tables`."""
    path = create_manifests(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename, date=date
    )
    path = str(path)
    click.echo(f"Combined workbook saved to: {path}")
    return path
