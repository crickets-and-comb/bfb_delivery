# noqa: D100
__doc__ = """
.. click:: bfb_delivery.cli.combine_route_tables:main
   :prog: combine_route_tables
   :nested: full
"""

import logging

import click
from typeguard import typechecked

from bfb_delivery import create_manifests
from bfb_delivery.lib.constants import Defaults, DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command(help=DocStrings.CREATE_MANIFESTS.cli_docstring)
@click.option(
    "--input_dir", type=str, required=True, help=DocStrings.CREATE_MANIFESTS.args["input_dir"]
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["output_dir"],
    help=DocStrings.CREATE_MANIFESTS.args["output_dir"],
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["output_filename"],
    help=DocStrings.CREATE_MANIFESTS.args["output_filename"],
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.CREATE_MANIFESTS["extra_notes_file"],
    help=DocStrings.CREATE_MANIFESTS.args["extra_notes_file"],
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
