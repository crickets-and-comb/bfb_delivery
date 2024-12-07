"""Public functions wrap internal functions which wrap library functions.

This allows separation of API from implementation. It also allows a simplified public API
separate from a more complex internal API with more options for power users.
"""

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
    Writes adjacent to the original workbook.

    Args:
        input_path: Path to the chunked route sheet that this function reads in and splits up.
        output_dir: Directory to save the output workbook.
            Empty string saves to the input `input_path` directory.
        output_filename: Name of the output workbook.
            Empty string sets filename to "split_workbook_{date}_{i of n_books}.xlsx".
        n_books: Number of workbooks to split into.

    Returns:
        Paths to the split chunked route workbooks.
    """
    return internal.split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
    )
