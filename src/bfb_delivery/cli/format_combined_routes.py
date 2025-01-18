"""format_combined_routes CLI. See :doc:`format_combined_routes` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import format_combined_routes
from bfb_delivery.lib.constants import Defaults


@click.command()
@click.option("--input_path", required=True, help="The path to the combined routes table.")
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.FORMAT_COMBINED_ROUTES["output_dir"],
    help=(
        "The directory to write the formatted table to. Empty string (default) saves "
        "to the input path's parent directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.FORMAT_COMBINED_ROUTES["output_filename"],
    help=(
        "The name of the formatted workbook. Empty string (default) will name the file "
        '"formatted_routes_{date}.xlsx".'
    ),
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.FORMAT_COMBINED_ROUTES["extra_notes_file"],
    help=(
        "The path to the extra notes file. If empty (default), uses a constant DataFrame. "
        "See :py:data:`bfb_delivery.lib.constants.ExtraNotes`."
    ),
)
@typechecked
def main(
    input_path: str, output_dir: str, output_filename: str, extra_notes_file: str
) -> str:
    """See public docstring: :py:func:`bfb_delivery.api.public.format_combined_routes`."""
    path = format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )
    click.echo(f"Formatted driver manifest saved to: {path.resolve()}")

    return str(path)
