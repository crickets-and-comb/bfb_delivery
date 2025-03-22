# noqa: D100
__doc__ = """
.. click:: bfb_delivery.cli.create_manifests_from_circuit:main
    :prog: create_manifests_from_circuit
    :nested: full
"""

import logging

import click
from typeguard import typechecked

from bfb_delivery import create_manifests_from_circuit
from bfb_delivery.lib.constants import Defaults, DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command(help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.cli_docstring)
@click.option(
    "--start_date",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["start_date"],
    help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.args["start_date"],
)
@click.option(
    "--end_date",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["end_date"],
    help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.args["end_date"],
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["output_dir"],
    help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.args["output_dir"],
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["output_filename"],
    help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.args["output_filename"],
)
# TODO: Change circuit_output_dir default to subdir in output_dir.
# https://github.com/crickets-and-comb/bfb_delivery/issues/56
@click.option(
    "--circuit_output_dir",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["circuit_output_dir"],
    help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.args["circuit_output_dir"],
)
@click.option(
    "--all_hhs",
    is_flag=True,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["all_hhs"],
    help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.args["all_hhs"],
)
@click.option(
    "--verbose",
    is_flag=True,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["verbose"],
    help="verbose: Flag to print verbose output.",
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS_FROM_CIRCUIT["extra_notes_file"],
    help=DocStrings.CREATE_MANIFESTS_FROM_CIRCUIT.args["extra_notes_file"],
)
@typechecked
def main(  # noqa: D103
    start_date: str,
    end_date: str,
    output_dir: str,
    output_filename: str,
    circuit_output_dir: str,
    all_hhs: bool,
    verbose: bool,
    extra_notes_file: str,
) -> str:
    final_manifest_path, new_circuit_output_dir = create_manifests_from_circuit(
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        output_filename=output_filename,
        circuit_output_dir=circuit_output_dir,
        all_hhs=all_hhs,
        verbose=verbose,
        extra_notes_file=extra_notes_file,
    )
    logger.info(f"Formatted workbook saved to:\n{final_manifest_path.resolve()}")
    # Print statement to capture in tests.
    print(str(final_manifest_path))
    print(str(new_circuit_output_dir))

    return str(final_manifest_path)
