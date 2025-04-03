"""Internal functions overlay library and are typically wrapped by public functions.

This allows us to maintain a separation of API from implementation.
Internal functions may come with extra options that public functions don't have, say for
power users and developers who may want to use an existing DB session or something.
"""

from pathlib import Path

from typeguard import typechecked

from bfb_delivery.lib.constants import DocStrings
from bfb_delivery.lib.dispatch import write_to_circuit
from bfb_delivery.lib.dispatch.read_circuit import get_route_files
from bfb_delivery.lib.formatting import sheet_shaping


def build_routes_from_chunked(  # noqa: D103
    input_path: str,
    output_dir: str,
    start_date: str,
    no_distribute: bool,
    verbose: bool,
    extra_notes_file: str,
) -> Path:
    return write_to_circuit.build_routes_from_chunked(
        input_path=input_path,
        output_dir=output_dir,
        start_date=start_date,
        no_distribute=no_distribute,
        verbose=verbose,
        extra_notes_file=extra_notes_file,
    )


build_routes_from_chunked.__doc__ = DocStrings.BUILD_ROUTES_FROM_CHUNKED.api_docstring


@typechecked
def split_chunked_route(  # noqa: D103
    input_path: Path | str,
    output_dir: Path | str,
    output_filename: str,
    n_books: int,
    book_one_drivers_file: str,
    date: str,
) -> list[Path]:
    return sheet_shaping.split_chunked_route(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        n_books=n_books,
        book_one_drivers_file=book_one_drivers_file,
        date=date,
    )


split_chunked_route.__doc__ = DocStrings.SPLIT_CHUNKED_ROUTE.api_docstring


@typechecked
def create_manifests_from_circuit(  # noqa: D103
    start_date: str,
    end_date: str,
    plan_ids: list[str],
    output_dir: str,
    output_filename: str,
    circuit_output_dir: str,
    all_hhs: bool,
    verbose: bool,
    extra_notes_file: str,
) -> tuple[Path, Path]:
    circuit_output_dir = get_route_files(
        start_date=start_date,
        end_date=end_date,
        plan_ids=plan_ids,
        output_dir=circuit_output_dir,
        all_hhs=all_hhs,
        verbose=verbose,
    )
    formatted_manifest_path = sheet_shaping.create_manifests(
        input_dir=circuit_output_dir,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )

    return formatted_manifest_path, Path(circuit_output_dir)


create_manifests_from_circuit.__doc__ = DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.api_docstring


@typechecked
def create_manifests(  # noqa: D103
    input_dir: Path | str, output_dir: Path | str, output_filename: str, extra_notes_file: str
) -> Path:
    formatted_manifest_path = sheet_shaping.create_manifests(
        input_dir=input_dir,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )

    return formatted_manifest_path


create_manifests.__doc__ = DocStrings.CREATE_MANIFESTS.api_docstring


@typechecked
def combine_route_tables(  # noqa: D103
    input_dir: Path | str, output_dir: Path | str, output_filename: str
) -> Path:
    return sheet_shaping.combine_route_tables(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename
    )


combine_route_tables.__doc__ = DocStrings.COMBINE_ROUTE_TABLES.api_docstring


@typechecked
def format_combined_routes(  # noqa: D103
    input_path: Path | str,
    output_dir: Path | str,
    output_filename: str,
    extra_notes_file: str,
) -> Path:
    return sheet_shaping.format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )


format_combined_routes.__doc__ = DocStrings.FORMAT_COMBINED_ROUTES.api_docstring
