"""format_combined_routes CLI. See :doc:`format_combined_routes` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import format_combined_routes


# TODO: Can we set the defaults as constants to sync with public?
@click.command()
@click.option("--input_path", required=True, help="The path to the combined routes table.")
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default="",
    help=(
        "The directory to write the formatted table to. Empty string (default) saves "
        "to the input path's parent directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default="",
    help=(
        "The name of the formatted workbook. Empty string (default) will name the file "
        '"formatted_routes_{date}.xlsx".'
    ),
)
@click.option(
    "--date",
    type=str,
    required=False,
    default="Dummy date",
    help=(
        "The date to use in driver manifests. Empty string (default) will use today's date "
        "as {MM.DD}'"
    ),
)
@typechecked
def main(input_path: str, output_dir: str, output_filename: str, date: str) -> str:
    """See public docstring: :py:func:`bfb_delivery.api.public.format_combined_routes`."""
    path = format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        date=date,
    )
    path = str(path)
    click.echo(f"Formatted driver manifest saved to: {path}")
    return path
