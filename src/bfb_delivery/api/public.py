"""Public functions wrap internal functions which wrap library functions.

This allows separation of API from implementation. It also allows a simplified public API
separate from a more complex internal API with more options for power users.
"""

from pathlib import Path

from typeguard import typechecked

from bfb_delivery.api import internal
from bfb_delivery.lib.constants import Defaults, DocStrings


def build_routes_from_chunked(  # noqa: D103
    input_path: str,
    output_dir: str = Defaults.BUILD_ROUTES_FROM_CHUNKED["output_dir"],
    start_date: str = Defaults.BUILD_ROUTES_FROM_CHUNKED["start_date"],
    no_distribute: bool = Defaults.BUILD_ROUTES_FROM_CHUNKED["no_distribute"],
    verbose: bool = Defaults.BUILD_ROUTES_FROM_CHUNKED["verbose"],
    extra_notes_file: str = Defaults.BUILD_ROUTES_FROM_CHUNKED["extra_notes_file"],
) -> Path:
    return internal.build_routes_from_chunked(
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
    output_dir: Path | str = Defaults.SPLIT_CHUNKED_ROUTE["output_dir"],
    output_filename: str = Defaults.SPLIT_CHUNKED_ROUTE["output_filename"],
    n_books: int = Defaults.SPLIT_CHUNKED_ROUTE["n_books"],
    book_one_drivers_file: str = Defaults.SPLIT_CHUNKED_ROUTE["book_one_drivers_file"],
    date: str = Defaults.SPLIT_CHUNKED_ROUTE["date"],
) -> list[Path]:
    return internal.split_chunked_route(
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
    start_date: str = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["start_date"],
    end_date: str = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["end_date"],
    plan_ids: list[str] = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["plan_ids"],
    output_dir: str = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["output_dir"],
    output_filename: str = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["output_filename"],
    circuit_output_dir: str = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["circuit_output_dir"],
    all_hhs: bool = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["all_hhs"],
    verbose: bool = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["verbose"],
    extra_notes_file: str = Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["extra_notes_file"],
) -> tuple[Path, Path]:
    final_manifest_path, new_circuit_output_dir = internal.create_manifests_from_circuit(
        start_date=start_date,
        end_date=end_date,
        plan_ids=plan_ids,
        output_dir=output_dir,
        output_filename=output_filename,
        circuit_output_dir=circuit_output_dir,
        all_hhs=all_hhs,
        verbose=verbose,
        extra_notes_file=extra_notes_file,
    )

    return final_manifest_path, new_circuit_output_dir


create_manifests_from_circuit.__doc__ = DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.api_docstring


@typechecked
def create_manifests(  # noqa: D103
    input_dir: Path | str,
    output_dir: Path | str = Defaults.CREATE_MANIFESTS["output_dir"],
    output_filename: str = Defaults.CREATE_MANIFESTS["output_filename"],
    extra_notes_file: str = Defaults.CREATE_MANIFESTS["extra_notes_file"],
) -> Path:
    formatted_manifest_path = internal.create_manifests(
        input_dir=input_dir,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )

    return formatted_manifest_path


create_manifests.__doc__ = DocStrings.CREATE_MANIFESTS.api_docstring


@typechecked
def combine_route_tables(  # noqa: D103
    input_dir: Path | str,
    output_dir: Path | str = Defaults.COMBINE_ROUTE_TABLES["output_dir"],
    output_filename: str = Defaults.COMBINE_ROUTE_TABLES["output_filename"],
) -> Path:
    return internal.combine_route_tables(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename
    )


combine_route_tables.__doc__ = DocStrings.COMBINE_ROUTE_TABLES.api_docstring


@typechecked
def format_combined_routes(  # noqa: D103
    input_path: Path | str,
    output_dir: Path | str = Defaults.FORMAT_COMBINED_ROUTES["output_dir"],
    output_filename: str = Defaults.FORMAT_COMBINED_ROUTES["output_filename"],
    extra_notes_file: str = Defaults.FORMAT_COMBINED_ROUTES["extra_notes_file"],
) -> Path:
    return internal.format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )


format_combined_routes.__doc__ = DocStrings.FORMAT_COMBINED_ROUTES.api_docstring
