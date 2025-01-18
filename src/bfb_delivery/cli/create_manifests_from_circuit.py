"""combine_route_tables CLI. See :doc:`create_manifests` for more information."""

import click
from typeguard import typechecked

from bfb_delivery import create_manifests_from_circuit
from bfb_delivery.lib.constants import Defaults


@click.command()
@click.option(
    "--start_date",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["start_date"],
    help=(
        'The start date to use in the output workbook sheetnames as "YYYYMMDD".'
        "Empty string (default) uses the soonest Friday. Range is inclusive."
    ),
)
@click.option(
    "--end_date",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["end_date"],
    help=(
        'The end date to use in the output workbook sheetnames as "YYYYMMDD".'
        "Empty string (default) uses the start date. Range is inclusive."
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
    "--all_HHs",
    type=bool,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["all_HHs"],
    help=(
        'Flag to get only the "All HHs" route.'
        'False gets all routes except "All HHs". True gets only the "All HHs" route.'
        "NOTE: True returns email column in CSVs, for reuploading after splitting."
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
    start_date: str,
    end_date: str,
    output_dir: str,
    output_filename: str,
    circuit_output_dir: str,
    all_HHs: bool,
    extra_notes_file: str,
) -> str:
    """See public docstring.

    :py:func:`bfb_delivery.api.public.create_manifests_from_circuit`.

    """
    final_manifest_path = create_manifests_from_circuit(
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        output_filename=output_filename,
        circuit_output_dir=circuit_output_dir,
        all_HHs=all_HHs,
        extra_notes_file=extra_notes_file,
    )
    final_manifest_path = str(final_manifest_path)
    click.echo(f"Formatted workbook saved to: {final_manifest_path}")

    return final_manifest_path
