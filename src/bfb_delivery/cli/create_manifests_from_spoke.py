# noqa: D100
__doc__ = """
.. click:: bfb_delivery.cli.create_manifests_from_spoke:main
    :prog: create_manifests_from_spoke
    :nested: full
"""

import logging

import click
from typeguard import typechecked

from bfb_delivery import create_manifests_from_spoke
from bfb_delivery.lib.constants import DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command(help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.cli_docstring)
@click.option(
    "--start_date",
    type=str,
    required=False,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["start_date"],
    help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.args["start_date"],
)
@click.option(
    "--end_date",
    type=str,
    required=False,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["end_date"],
    help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.args["end_date"],
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["output_dir"],
    help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.args["output_dir"],
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["output_filename"],
    help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.args["output_filename"],
)
# TODO: Change spoke_output_dir default to subdir in output_dir.
# https://github.com/crickets-and-comb/bfb_delivery/issues/56
@click.option(
    "--spoke_output_dir",
    type=str,
    required=False,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["spoke_output_dir"],
    help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.args["spoke_output_dir"],
)
@click.option(
    "--all_hhs",
    is_flag=True,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["all_hhs"],
    help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.args["all_hhs"],
)
@click.option(
    "--verbose",
    is_flag=True,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["verbose"],
    help="verbose: Flag to print verbose output.",
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.defaults["extra_notes_file"],
    help=DocStrings.CREATE_MANIFESTS_FROM_SPOKE.args["extra_notes_file"],
)
@typechecked
def main(  # noqa: D103
    start_date: str,
    end_date: str,
    output_dir: str,
    output_filename: str,
    spoke_output_dir: str,
    all_hhs: bool,
    verbose: bool,
    extra_notes_file: str,
) -> str:
    final_manifest_path, new_spoke_output_dir = create_manifests_from_spoke(
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        output_filename=output_filename,
        spoke_output_dir=spoke_output_dir,
        all_hhs=all_hhs,
        verbose=verbose,
        extra_notes_file=extra_notes_file,
    )
    logger.info(f"Formatted workbook saved to:\n{final_manifest_path.resolve()}")
    # Print statement to capture in tests.
    print(str(final_manifest_path))
    print(str(new_spoke_output_dir))

    return str(final_manifest_path)
