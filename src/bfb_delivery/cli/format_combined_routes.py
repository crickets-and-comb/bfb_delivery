"""format_combined_routes CLI. See :doc:`format_combined_routes` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import format_combined_routes


# TODO: Can we set the defaults as constants to sync with public?
# TODO: They may want to just pass a text file of the paths instead of multiple args.
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
@typechecked
def main(input_path: str, output_dir: str, output_filename: str) -> str:
    """See public docstring: :py:func:`bfb_delivery.api.public.format_combined_routes`."""
    path = format_combined_routes(
        input_path=input_path, output_dir=output_dir, output_filename=output_filename
    )
    path = str(path)
    click.echo(f"Formatted workbook saved to: {path}")
    return path
