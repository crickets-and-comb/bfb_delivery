"""combine_route_tables CLI. See :doc:`create_manifests` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import create_manifests_from_circuit
from bfb_delivery.lib.constants import Defaults


@click.command()
@click.option(
    "--date",
    type=str,
    required=False,
    help=(
        'The date to use in the output workbook sheetnames as "YYYYMMDD".'
        "Empty string (default) uses the soonest Friday."
    ),
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["output_dir"],
    help=(
        "The directory to write the output workbook to. Empty string (default) saves "
        "to present working directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["output_filename"],
    help=(
        "The name of the output workbook. Empty string (default) will name the file "
        '"formatted_routes_{date}.xlsx".'
    ),
)
@click.option(
    "--circuit_output_dir",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["circuit_output_dir"],
    help=(
        "The directory to save the Circuit route CSVs to."
        'Empty string saves to "routes_{date}" directory in present working directory.'
        "If the directory does not exist, it is created. If it exists, it is overwritten."
    ),
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["extra_notes_file"],
    help=(
        "The path to the extra notes file. If empty (default), uses a constant DataFrame. "
        "See :py:data:`bfb_delivery.lib.constants.ExtraNotes`."
    ),
)
@typechecked
def main(
    date: str,
    output_dir: str,
    output_filename: str,
    circuit_output_dir: str,
    extra_notes_file: str,
) -> str:
    """See public docstring.

    :py:func:`bfb_delivery.api.public.create_manifests_from_circuit`.

    """
    path = create_manifests_from_circuit(
        date=date,
        output_dir=output_dir,
        output_filename=output_filename,
        circuit_output_dir=circuit_output_dir,
        extra_notes_file=extra_notes_file,
    )
    path = str(path)
    click.echo(f"Formatted workbook saved to: {path}")

    return path
