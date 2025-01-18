"""split_chunked_route CLI. See :doc:`split_chunked_route` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import split_chunked_route
from bfb_delivery.lib.constants import Defaults


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
    default=Defaults.SPLIT_CHUNKED_ROUTE["output_dir"],
    help=(
        "Directory to save the output workbook. Empty string (default) saves to "
        "the input `input_path` directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["output_filename"],
    help=(
        "Name of the output workbook. Empty string (default) sets filename to "
        '"split_workbook_{date}_{i of n_books}.xlsx".'
    ),
)
@click.option(
    "--n_books",
    type=int,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["n_books"],
    help="Number of workbooks to split into. Default is 4.",
)
@click.option(
    "--book_one_drivers_file",
    type=str,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["book_one_drivers_file"],
    help=(
        "Path to the book-one driver's file. If empty (default), uses a constant list. "
        "See :py:data:`bfb_delivery.lib.constants.BookOneDrivers`."
    ),
)
@click.option(
    "--date",
    type=str,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["date"],
    help="The date to use in the output workbook sheetnames.",
)
@typechecked
def main(
    input_path: str,
    output_dir: str,
    output_filename: str,
    n_books: int,
    book_one_drivers_file: str,
    date: str,
) -> list[str]:
    """See public docstring: :py:func:`bfb_delivery.api.public.split_chunked_route`."""
    paths = split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
        book_one_drivers_file=book_one_drivers_file,
        date=date,
    )
    return_paths = [str(path.resolve()) for path in paths]
    click.echo(f"Split workbook(s) saved to: {return_paths}")

    return return_paths
