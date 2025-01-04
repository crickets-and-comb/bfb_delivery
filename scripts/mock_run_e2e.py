"""Mock the workflow end to end."""

# TODO: Make a real e2e test in the test suite.
import shutil
from pathlib import Path
from typing import Final

import click
import pandas as pd
from typeguard import typechecked

from bfb_delivery import (
    combine_route_tables,
    create_manifests,
    format_combined_routes,
    split_chunked_route,
)
from bfb_delivery.lib.constants import Columns

OUTPUT_DIRS: Final[dict[str, str]] = {
    "SPLIT_CHUNKED_DIR": ".test_data/split_chunked",
    "CIRCUIT_TABLES_DIR": ".test_data/circuit_tables",
    "COMBINED_TABLES_DIR": ".test_data/combined_tables",
    "FORMATTED_TABLES_DIR": ".test_data/formatted_tables",
    "MANIFESTS_DIR": ".test_data/manifests",
}


@click.command()
@click.option(
    "--mock_raw_chunked_sheet_path",
    type=str,
    required=True,
    help="Path to the raw chunked route sheet that this function reads in and splits up.",
)
def main(mock_raw_chunked_sheet_path: str) -> None:
    """Mock the workflow end to end.

    Use this to test on real data that can't be saved to the tests or uploaded to Circuit.

    This script takes the raw chunked workbook, splits it into workbooks to upload to Circuit,
    mocks the CSVs returned by Circuit, recombines them, and formats them.
    """
    for output_dir in OUTPUT_DIRS.values():
        # Remove directory if it exists.
        shutil.rmtree(output_dir, ignore_errors=True)
        Path(output_dir).mkdir(parents=True)

    split_chunked_sheet_paths = split_chunked_route(
        input_path=mock_raw_chunked_sheet_path,
        output_dir=OUTPUT_DIRS["SPLIT_CHUNKED_DIR"],
        n_books=4,
    )
    click.echo(
        f"Split chunked route saved to: {[str(path) for path in split_chunked_sheet_paths]}"
    )

    output_paths = mock_route_tables(
        split_chunked_sheet_paths=split_chunked_sheet_paths,
        output_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
    )
    output_dir = Path(output_paths[0]).parent
    click.echo(f"Mocked driver route tables saved to: {output_dir}")

    combined_path = combine_route_tables(
        input_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
        output_dir=OUTPUT_DIRS["COMBINED_TABLES_DIR"],
    )
    click.echo(f"Combined workbook saved to: {combined_path}")

    formatted_path = format_combined_routes(
        input_path=combined_path, output_dir=OUTPUT_DIRS["FORMATTED_TABLES_DIR"]
    )
    click.echo(f"Formatted workbook saved to: {formatted_path}")

    final_manifests_path = create_manifests(
        input_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"], output_dir=OUTPUT_DIRS["MANIFESTS_DIR"]
    )
    click.echo(
        f"Manifests workbook (same as formatted workbook) saved to: {final_manifests_path}"
    )


@typechecked
def mock_route_tables(
    split_chunked_sheet_paths: list[Path | str], output_dir: str
) -> list[str]:
    """Mock the driver route tables returned by Circuit.

    This function takes the split chunked workbooks and mocks the CSVs returned by Circuit.

    After splitting the chunked route into multiple books with multiple driver sheets,
    the sheets are uploaded to Circuit. Circuit then returns the driver route tables
    as CSVs, one per driver.

    Args:
        split_chunked_sheet_paths: The paths to the split chunked route sheets.
        output_dir: The directory to write the mocked driver route tables to.

    Returns:
        The paths to the mocked driver route tables.
    """
    output_paths = []
    for sheet_path in split_chunked_sheet_paths:
        with pd.ExcelFile(sheet_path) as xls:
            for sheet_name in xls.sheet_names:
                output_path = output_dir + f"/{sheet_name}.csv"
                output_paths.append(output_path)
                df = pd.read_excel(xls, sheet_name)
                df[Columns.STOP_NO] = [i + 1 for i in range(len(df))]
                df.to_csv(output_path, index=False)

    return output_paths


if __name__ == "__main__":
    main()
