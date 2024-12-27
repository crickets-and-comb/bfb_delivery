"""split_chunked_route CLI. See :doc:`split_chunked_route` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import split_chunked_route


# TODO: Can we set the defaults as constants to sync with public?
@click.command()
@click.option(
    "--input_path",
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
        "the input `input_path` directory."
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
def main(input_path: str, output_dir: str, output_filename: str, n_books: int) -> list[str]:
    """See public docstring: :py:func:`bfb_delivery.api.public.split_chunked_route`."""
    paths = split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
    )
    return_paths = [str(path) for path in paths]
    click.echo(f"Split workbook(s) saved to: {return_paths}")

    return return_paths
