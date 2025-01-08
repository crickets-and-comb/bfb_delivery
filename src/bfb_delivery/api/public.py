"""Public functions wrap internal functions which wrap library functions.

This allows separation of API from implementation. It also allows a simplified public API
separate from a more complex internal API with more options for power users.
"""

from pathlib import Path

from typeguard import typechecked

from bfb_delivery.api import internal
from bfb_delivery.lib.constants import Defaults


@typechecked
def split_chunked_route(
    input_path: Path | str,
    output_dir: Path | str = Defaults.SPLIT_CHUNKED_ROUTE["output_dir"],
    output_filename: str = Defaults.SPLIT_CHUNKED_ROUTE["output_filename"],
    n_books: int = Defaults.SPLIT_CHUNKED_ROUTE["n_books"],
    book_one_drivers_file: str = Defaults.SPLIT_CHUNKED_ROUTE["book_one_drivers_file"],
) -> list[Path]:
    """Split route sheet into n workbooks with sheets by driver.

    Sheets by driver allows splitting routes by driver on Circuit upload.
    Multiple workbooks allows team to split the uploads among members, so one person
    doesn't have to upload all routes.
    This process follows the "chunking" process in the route generation, where routes
    are split into smaller "chunks" by driver (i.e., each stop is labeled with a driver).

    Reads a route spreadsheet at `input_path`.
    Writes `n_books` Excel workbooks with each sheet containing the stops for a single driver.
    Writes adjacent to the original workbook unless `output_dir` specified. If specified, will
    create the directory if it doesn't exist.

    Note: Renames "Box Type" column name to "Product Type", per Circuit API.

    See :doc:`split_chunked_route` for more information.

    Args:
        input_path: Path to the chunked route sheet that this function reads in and splits up.
        output_dir: Directory to save the output workbook.
            Empty string saves to the input `input_path` directory.
        output_filename: Name of the output workbook.
            Empty string sets filename to "split_workbook_{date}_{i of n_books}.xlsx".
        n_books: Number of workbooks to split into.
        book_one_drivers_file: Path to the book-one driver's file. If empty (default), uses
            a constant list.

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
        book_one_drivers_file=book_one_drivers_file,
    )


@typechecked
def create_manifests(
    input_dir: Path | str,
    output_dir: Path | str = Defaults.CREATE_MANIFESTS["output_dir"],
    output_filename: str = Defaults.CREATE_MANIFESTS["output_filename"],
    date: str = Defaults.CREATE_MANIFESTS["date"],
) -> Path:
    """From Circuit route CSVs, creates driver manifest workbook ready to print.

    This is used after optimizing and exporting the routes to individual CSVs. Reads in
    driver route CSVs from `input_dir` and creates a formatted workbook with driver
    manifests ready to print, with headers, aggregate data, and color-coded box types. Each
    driver's route is a separate sheet in the workbook.

    The workbook is saved to `output_dir` with the name `output_filename`. Will create
    `output_dir` if it doesn't exist.

    The date is used in the manifest headers and sheet names, not in the filename.

    Just wraps :py:func:`bfb_delivery.api.public.combine_route_tables` and
    :py:func:`bfb_delivery.api.public.format_combined_routes`. Creates an intermediate output
    workbook with all routes combined, then formats it.

    See :doc:`create_manifests` for more information.

    Args:
        input_dir: The directory containing the driver route CSVs.
        output_dir: The directory to write the formatted manifest workbook to.
            Empty string saves to the input `input_dir` directory.
        output_filename: The name of the output workbook.
            Empty string sets filename to "final_manifests_{date}.xlsx".
        date: The date to use in the driver manifests.

    Returns:
        Path to the formatted manifest workbook.
    """
    formatted_manifest_path = internal.create_manifests(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename, date=date
    )

    return formatted_manifest_path


@typechecked
def combine_route_tables(
    input_dir: Path | str,
    output_dir: Path | str = Defaults.COMBINE_ROUTE_TABLES["output_dir"],
    output_filename: str = Defaults.COMBINE_ROUTE_TABLES["output_filename"],
) -> Path:
    """Combines the driver route CSVs into a single workbook.

    This is used after optimizing and exporting the routes to individual CSVs. It prepares the
    worksheets to be formatted with :py:func:`bfb_delivery.api.public.format_combined_routes`.

    If `output_dir` is specified, will create the directory if it doesn't exist.

    Note: Changes "Product Type" column name back to "Box Type".

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


@typechecked
def format_combined_routes(
    input_path: Path | str,
    output_dir: Path | str = Defaults.FORMAT_COMBINED_ROUTES["output_dir"],
    output_filename: str = Defaults.FORMAT_COMBINED_ROUTES["output_filename"],
    date: str = Defaults.FORMAT_COMBINED_ROUTES["date"],
) -> Path:
    """Formats the combined routes table into driver manifests to print.

    Adds headers and aggregate data. Color codes box types.

    This is used after combining the driver route CSVs into a single workbook
    using :py:func:`bfb_delivery.api.public.combine_route_tables`.

    If `output_dir` is specified, will create the directory if it doesn't exist.

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
    return internal.format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        date=date,
    )


# TODO: Wrap format and combine functions in a single function.
