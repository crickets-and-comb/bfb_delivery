# noqa: D100
__doc__ = """
.. click:: bfb_delivery.cli.build_routes_from_chunked:main
    :prog: create_manifests_from_circuit
    :nested: full
"""

import logging

import click
from typeguard import typechecked

from bfb_delivery import build_routes_from_chunked
from bfb_delivery.lib.constants import Defaults, DocStrings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command(help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.cli_docstring)
@click.option(
    "--input_path",
    type=str,
    required=True,
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["input_path"],
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["output_dir"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["output_dir"],
)
@click.option(
    "--start_date",
    type=str,
    required=False,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["start_date"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["start_date"],
)
@click.option(
    "--no_distribute",
    is_flag=True,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["no_distribute"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["no_distribute"],
)
@click.option(
    "--verbose",
    is_flag=True,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["verbose"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["verbose"],
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["extra_notes_file"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["extra_notes_file"],
)
@typechecked
def main(  # noqa: D103
    input_path: str,
    output_dir: str,
    start_date: str,
    no_distribute: bool,
    verbose: bool,
    extra_notes_file: str,
) -> str:
    final_manifest_path = build_routes_from_chunked(
        input_path=input_path,
        output_dir=output_dir,
        start_date=start_date,
        no_distribute=no_distribute,
        verbose=verbose,
        extra_notes_file=extra_notes_file,
    )
    logger.info(f"Formatted workbook saved to:\n{final_manifest_path.resolve()}")
    # Print statement to capture in tests.
    print(str(final_manifest_path))

    return str(final_manifest_path)
