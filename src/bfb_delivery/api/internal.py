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
    input_path: Path | str, output_dir: Path | str, output_filename: str, n_books: int
) -> list[Path]:
    """See public docstring: :py:func:`bfb_delivery.api.public.split_chunked_route`."""
    return sheet_shaping.split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
    )


@typechecked
def combine_route_tables(
    input_dir: Path | str, output_dir: Path | str, output_filename: str
) -> Path:
    """See public docstring: :py:func:`bfb_delivery.api.public.combine_route_tables`."""
    return sheet_shaping.combine_route_tables(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename
    )


@typechecked
def format_combined_routes(
    input_path: Path | str,
    output_dir: Path | str = "",
    output_filename: str = "",
    date: str = "Dummy date",
) -> Path:
    """See public docstring: :py:func:`bfb_delivery.api.public.format_combined_routes`."""
    return sheet_shaping.format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        date=date,
    )
