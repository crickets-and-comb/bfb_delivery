"""Internal functions overlay library and are typically wrapped by public functions.

This allows us to maintain a separation of API from implementation.
Internal functions may come with extra options that public functions don't have, say for
power users and developers who may want to use an existing DB session or something.
"""

from pathlib import Path

from typeguard import typechecked

from bfb_delivery.lib.formatting import sheet_shaping


@typechecked
def split_chunked_route(
    sheet_path: Path | str, output_dir: Path | str, output_filename: str, n_books: int
) -> list[Path]:
    """See public docstring."""
    return sheet_shaping.split_chunked_route(
        sheet_path=sheet_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
    )
