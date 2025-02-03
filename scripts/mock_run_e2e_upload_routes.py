"""Mock run of the E2E upload routes."""

import subprocess

import click

from bfb_delivery.lib.constants import Defaults, DocStrings


@click.command()
@click.option(
    "--input_path",
    type=str,
    required=False,
    default=".test_data/scratch/test_master_chunked.xlsx",
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["input_path"],
)
@click.option(
    "--output_dir",
    type=str,
    required=False,
    default=".test_data/scratch",
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
    "--distribute",
    is_flag=True,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["distribute"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["distribute"],
)
@click.option(
    "--verbose",
    is_flag=True,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["verbose"],
    help="verbose: Flag to print verbose output.",
)
@click.option(
    "--book_one_drivers_file",
    type=str,
    required=False,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["book_one_drivers_file"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["book_one_drivers_file"],
)
@click.option(
    "--extra_notes_file",
    type=str,
    required=False,
    default=Defaults.BUILD_ROUTES_FROM_CHUNKED["extra_notes_file"],
    help=DocStrings.BUILD_ROUTES_FROM_CHUNKED.args["extra_notes_file"],
)
def main(
    input_path: str,
    output_dir: str,
    start_date: str,
    distribute: bool,
    verbose: bool,
    book_one_drivers_file: str,
    extra_notes_file: str,
) -> None:
    """Run the main function."""
    args_list = [
        "--input_path",
        input_path,
        "--start_date",
        start_date,
        "--output_dir",
        output_dir,
        "--book_one_drivers_file",
        book_one_drivers_file,
        "--extra_notes_file",
        extra_notes_file,
    ]
    if distribute:
        args_list.append("--distribute")
    if verbose:
        args_list.append("--verbose")

    result = subprocess.run(["build_routes_from_chunked"] + args_list)
    print(f"result: {result}")


if __name__ == "__main__":
    main()
