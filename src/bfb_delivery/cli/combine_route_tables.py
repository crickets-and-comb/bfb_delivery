# noqa: D100
__doc__ = """
.. click:: bfb_delivery.cli.combine_route_tables:main
   :prog: combine_route_tables
   :nested: full
"""
import logging

import click
from typeguard import typechecked

from bfb_delivery import combine_route_tables
from bfb_delivery.lib.constants import Defaults, DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command(help=DocStrings.COMBINE_ROUTE_TABLES.cli_docstring)
@click.option(
    "--input_dir",
    type=str,
    required=True,
    help=DocStrings.COMBINE_ROUTE_TABLES.args["input_dir"],
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.COMBINE_ROUTE_TABLES["output_dir"],
    help=DocStrings.COMBINE_ROUTE_TABLES.args["output_dir"],
)
@click.option(
    "--output_filename",
    type=str,
    required=False,
    default=Defaults.COMBINE_ROUTE_TABLES["output_filename"],
    help=DocStrings.COMBINE_ROUTE_TABLES.args["output_filename"],
)
@typechecked
def main(input_dir: str, output_dir: str, output_filename: str) -> str:  # noqa: D103
    path = combine_route_tables(
        input_dir=input_dir, output_dir=output_dir, output_filename=output_filename
    )
    logger.info(f"Combined workbook saved to:\n{path.resolve()}")

    return str(path)
