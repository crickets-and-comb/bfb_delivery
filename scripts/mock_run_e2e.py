# TODO: Make a real e2e test in the test suite.
from pathlib import Path
import click
import pandas as pd
from typing import Final
from typeguard import typechecked

from bfb_delivery import combine_route_tables, split_chunked_route

SPLIT_CHUNKED_DIR: Final[str] = ".test_data/split_chunked"
CIRCUIT_TABLES_DIR: Final[str] = ".test_data/circuit_tables"
COMBINED_TABLES_DIR: Final[str] = ".test_data/combined_tables"

@click.command()
@click.option(
    "--mock_raw_chunked_sheet_path",
    type=str,
    required=True,
    help="Path to the raw chunked route sheet that this function reads in and splits up.",
)
def main(mock_raw_chunked_sheet_path: str) -> None:
    """Mock the workflow end to end.

    Use this for testing on real data that can't be saved to the test suite or uploaded to Circuit.

    This script takes the raw chunked workbook, splits it into workbooks to upload to Circuit,
    mocks the CSVs returned by Circuit, and recombines them.
    """
    Path(SPLIT_CHUNKED_DIR).mkdir(parents=True, exist_ok=True)
    Path(CIRCUIT_TABLES_DIR).mkdir(parents=True, exist_ok=True)
    Path(COMBINED_TABLES_DIR).mkdir(parents=True, exist_ok=True)

    split_chunked_sheet_paths = split_chunked_route(input_path=mock_raw_chunked_sheet_path, output_dir=SPLIT_CHUNKED_DIR, n_books=4)
    click.echo(f"Split chunked route saved to: {[str(path) for path in split_chunked_sheet_paths]}")

    output_paths = mock_route_tables(split_chunked_sheet_paths=split_chunked_sheet_paths, output_dir=CIRCUIT_TABLES_DIR)
    click.echo(f"Mocked driver route tables saved to: {[str(path) for path in output_paths]}")

    combined_path = combine_route_tables(input_paths=[str(path) for path in output_paths], output_dir=COMBINED_TABLES_DIR)
    click.echo(f"Combined workbook saved to: {combined_path}")
    # TODO: Add formatting step once it's implemented.


@typechecked
def mock_route_tables(split_chunked_sheet_paths: list[Path | str], output_dir: str) -> list[str]:
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
                df.to_csv(output_path, index=False)

    return output_paths

if __name__ == "__main__":
    main()