# noqa: D100
__doc__ = """
.. click:: bfb_delivery.cli.split_chunked_route:main
   :prog: split_chunked_route
   :nested: full
"""
# Adding necessary Imports
import sys

import logging

import click
from typeguard import typechecked

from bfb_delivery import split_chunked_route
from bfb_delivery.lib.constants import Defaults, DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Creating a function to validate colums beforehand!
def validate_columns(df, required_columns):
    """
    Validates that the DataFrame has the required columns with correct names and no duplicates.
    """
    # Check for required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        sys.exit(f"Error: Missing required columns: {', '.join(missing_columns)}")

    # Check for duplicate columns
    if df.columns.duplicated().any():
        sys.exit("Error: Duplicate columns found. Please ensure each column has a unique name.")

@click.command(help=DocStrings.SPLIT_CHUNKED_ROUTE.cli_docstring)
@click.option(
    "--input_path",
    type=str,
    required=True,
    help=DocStrings.SPLIT_CHUNKED_ROUTE.args["input_path"],
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["output_dir"],
    help=DocStrings.SPLIT_CHUNKED_ROUTE.args["output_dir"],
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["output_filename"],
    help=DocStrings.SPLIT_CHUNKED_ROUTE.args["output_filename"],
)
@click.option(
    "--n_books",
    type=int,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["n_books"],
    help=DocStrings.SPLIT_CHUNKED_ROUTE.args["n_books"],
)
@click.option(
    "--book_one_drivers_file",
    type=str,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["book_one_drivers_file"],
    help=DocStrings.SPLIT_CHUNKED_ROUTE.args["book_one_drivers_file"],
)
@click.option(
    "--date",
    type=str,
    required=False,
    default=Defaults.SPLIT_CHUNKED_ROUTE["date"],
    help=DocStrings.SPLIT_CHUNKED_ROUTE.args["date"],
)
@typechecked
def main(  # noqa: D103
    input_path: str,
    output_dir: str,
    output_filename: str,
    n_books: int,
    book_one_drivers_file: str,
    date: str,
) -> list[str]:
    paths = split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
        book_one_drivers_file=book_one_drivers_file,
        date=date,
    )
    return_paths = [str(path.resolve()) for path in paths]
    logger.info(f"Split workbook(s) saved to:\n{return_paths}")

    return return_paths
