"""Public functions wrap internal functions which wrap library functions.

This allows separation of API from implementation. It also allows a simplified public API
separate from a more complex internal API with more options for power users.
"""

from logging import warning
from pathlib import Path

from typeguard import typechecked

from bfb_delivery.api import internal


@typechecked
def split_chunked_route(
    input_path: Path | str,
    output_dir: Path | str = "",
    output_filename: str = "",
    n_books: int = 4,
) -> list[Path]:
    """Split route sheet into n workbooks with sheets by driver.

    Sheets by driver allows splitting routes by driver on Circuit upload.
    Multiple workbooks allows team to split the uploads among members, so one person
    doesn't have to upload all routes.
    This process follows the "chunking" process in the route generation, where routes
    are split into smaller "chunks" by driver (i.e., each stop is labeled with a driver).

    Reads a route spreadsheet at `input_path`.
    Writes `n_books` Excel workbooks with each sheet containing the stops for a single driver.
    Writes adjacent to the original workbook unless `output_dir` specified.

    See :doc:`split_chunked_route` for more information.

    Args:
        input_path: Path to the chunked route sheet that this function reads in and splits up.
        output_dir: Directory to save the output workbook.
            Empty string saves to the input `input_path` directory.
        output_filename: Name of the output workbook.
            Empty string sets filename to "split_workbook_{date}_{i of n_books}.xlsx".
        n_books: Number of workbooks to split into.

    Returns:
        Paths to the split chunked route workbooks.

    Raises:
        ValueError: If `n_books` is less than 1.
        ValueError: If `n_books` is greater than the number of drivers in the input workbook.
    """
    return internal.split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
    )


@typechecked
def combine_route_tables(
    input_dir: Path | str = "", output_dir: Path | str = "", output_filename: str = ""
) -> Path:
    """Combines the driver route CSVs into a single workbook.

    This is used after optimizing and exporting the routes to individual CSVs. It prepares the
    worksheets to be formatted with :py:func:`bfb_delivery.api.public.format_combined_routes`.

    See :doc:`combine_route_tables` for more information.

    Args:
        input_dir: The directory containing the driver route CSVs.
        output_dir: The directory to write the output workbook to.
            Empty string (default) saves to the first input path's parent directory.
        output_filename: The name of the output workbook.
            Empty string (default) will name the file "combined_routes_{date}.xlsx".

    Returns:
        The path to the output workbook.

    Raises:
        ValueError: If `input_paths` is empty.
    """
    return internal.combine_route_tables(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename
    )


# TODO: Update docs. (What until done, though.)
# TODO: Set default to empty string once usage confirmed with users.
@typechecked
def format_combined_routes(
    input_path: Path | str,
    output_dir: Path | str = "",
    output_filename: str = "",
    date: str = "Dummy date",
) -> Path:
    """Formats the combined routes table into driver manifests to print.

    Adds headers and aggregate data. Color codes box types.

    This is used after combining the driver route CSVs into a single workbook
    using :py:func:`bfb_delivery.api.public.combine_route_tables`.

    NOTE: This function is still under construction.

    See :doc:`format_combined_routes` for more information.

    Args:
        input_path: The path to the combined routes table.
        output_dir: The directory to write the formatted table to.
            Empty string (default) saves to the input path's parent directory.
        output_filename: The name of the formatted workbook.
            Empty string (default) will name the file "formatted_routes_{date}.xlsx".
        date: The date to use in driver manifests. Empty string (default) will use today's
            date as {MM.DD}.

    Returns:
        The path to the formatted table.
    """
    warning("This function is still under construction.")
    return internal.format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        date=date,
    )


# TODO: Wrap format and combine functions in a single function.
