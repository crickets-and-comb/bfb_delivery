"""combine_route_tables CLI. See :doc:`create_manifests` for more information."""

import logging

import click
from typeguard import typechecked

from bfb_delivery import create_manifests
from bfb_delivery.lib.constants import Defaults, DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command(help=DocStrings.CREATE_MANIFESTS)
@click.option(
    "--input_dir",
    type=str,
    required=True,
    help="The directory containing the driver route CSVs.",
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["output_dir"],
    help=(
        "The directory to write the output workbook to. Empty string (default) saves "
        "to the `input_dir` directory."
    ),
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["output_filename"],
    help=(
        "The name of the output workbook. Empty string (default) will name the file "
        '"final_manifests_{date}.xlsx".'
    ),
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["extra_notes_file"],
    help=(
        "The path to the extra notes file. If empty (default), uses a constant DataFrame. "
        "See :py:data:`bfb_delivery.lib.constants.ExtraNotes`."
    ),
)
@typechecked
def main(  # noqa: D103
    input_dir: str, output_dir: str, output_filename: str, extra_notes_file: str
) -> str:
    final_manifest_path = create_manifests(
        input_dir=input_dir,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )
    logger.info(f"Final manifests saved to:\n{final_manifest_path.resolve()}")

    return str(final_manifest_path)
