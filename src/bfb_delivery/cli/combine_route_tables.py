"""combine_route_tables CLI. See :doc:`combine_route_tables` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import combine_route_tables
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
    default=Defaults.COMBINE_ROUTE_TABLES["output_dir"],
    help=(
        "The directory to write the output workbook to. Empty string (default) saves "
        "to the `input_dir` directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.COMBINE_ROUTE_TABLES["output_filename"],
    help=(
        "The name of the output workbook. Empty string (default) will name the file "
        '"combined_routes_{date}.xlsx".'
    ),
)
@typechecked
def main(input_dir: str, output_dir: str, output_filename: str) -> str:
    """See public docstring: :py:func:`bfb_delivery.api.public.combine_route_tables`."""
    path = combine_route_tables(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename
    )
    click.echo(f"Combined workbook saved to: {path.resolve()}")

    return str(path)
