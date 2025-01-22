"""Internal functions overlay library and are typically wrapped by public functions.

This allows us to maintain a separation of API from implementation.
Internal functions may come with extra options that public functions don't have, say for
power users and developers who may want to use an existing DB session or something.
"""

from pathlib import Path

from typeguard import typechecked

from bfb_delivery.lib.dispatch.read_circuit import get_route_files
from bfb_delivery.lib.formatting import sheet_shaping


@typechecked
def split_chunked_route(
    input_path: Path | str,
    output_dir: Path | str,
    output_filename: str,
    n_books: int,
    book_one_drivers_file: str,
    date: str,
) -> list[Path]:
    """See public docstring: :py:func:`bfb_delivery.api.public.split_chunked_route`."""
    return sheet_shaping.split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
        book_one_drivers_file=book_one_drivers_file,
        date=date,
    )


@typechecked
def create_manifests_from_circuit(
    start_date: str,
    end_date: str,
    output_dir: str,
    output_filename: str,
    circuit_output_dir: str,
    all_HHs: bool,
    verbose: bool,
    extra_notes_file: str,
) -> Path:
    """See public docstring.

    :py:func:`bfb_delivery.api.public.create_manifests_from_circuit`.
    """
    input_dir = get_route_files(
        start_date=start_date,
        end_date=end_date,
        output_dir=circuit_output_dir,
        all_HHs=all_HHs,
        verbose=verbose,
    )
    formatted_manifest_path = sheet_shaping.create_manifests(
        input_dir=input_dir,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )

    return formatted_manifest_path


@typechecked
def create_manifests(
    input_dir: Path | str, output_dir: Path | str, output_filename: str, extra_notes_file: str
) -> Path:
    """See public docstring for :py:func:`bfb_delivery.api.public.create_manifests`."""
    formatted_manifest_path = sheet_shaping.create_manifests(
        input_dir=input_dir,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )

    return formatted_manifest_path


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
    output_dir: Path | str,
    output_filename: str,
    extra_notes_file: str,
) -> Path:
    """See public docstring: :py:func:`bfb_delivery.api.public.format_combined_routes`."""
    return sheet_shaping.format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )
