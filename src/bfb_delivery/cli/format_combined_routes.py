# noqa: D100
__doc__ = """
.. click:: bfb_delivery.cli.format_combined_routes:main
   :prog: format_combined_routes
   :nested: full
"""
import logging

import click
from typeguard import typechecked

from bfb_delivery import format_combined_routes
from bfb_delivery.lib.constants import Defaults, DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command(help=DocStrings.FORMAT_COMBINED_ROUTES.cli_docstring)
@click.option("--input_path", required=True, help="The path to the combined routes table.")
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.FORMAT_COMBINED_ROUTES["output_dir"],
    help=DocStrings.FORMAT_COMBINED_ROUTES.args["output_dir"],
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.FORMAT_COMBINED_ROUTES["output_filename"],
    help=DocStrings.FORMAT_COMBINED_ROUTES.args["output_filename"],
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.FORMAT_COMBINED_ROUTES["extra_notes_file"],
    help=DocStrings.FORMAT_COMBINED_ROUTES.args["extra_notes_file"],
)
@typechecked
def main(  # noqa: D103
    input_path: str, output_dir: str, output_filename: str, extra_notes_file: str
) -> str:
    path = format_combined_routes(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename,
        extra_notes_file=extra_notes_file,
    )
    logger.info(f"Formatted driver manifest saved to:\n{path.resolve()}")

    return str(path)
