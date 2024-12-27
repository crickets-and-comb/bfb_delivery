from pathlib import Path
import click
import pandas as pd

@click.command()
@click.option(
    "--split_chunked_sheet_paths",
    multiple=True,
    required=True,
    help="The paths to the split chunked route sheets.",
)
def main(split_chunked_sheet_paths: tuple[str, ...]):
    """Mock the driver route tables returned by Circuit."""
    output_paths = mock_route_tables(split_chunked_sheet_paths)
    click.echo(f"Mocked driver route tables saved to: {output_paths}")

def mock_route_tables(split_chunked_sheet_paths: list[Path | str]) -> list[str]:
    """Mock the driver route tables returned by Circuit.
    
    After splitting the chunked route into multiple books with multiple driver sheets,
    the sheets are uploaded to Circuit. Circuit then returns the driver route tables
    as CSVs, one per driver. This function takes the split chunked workbooks and mocks
    the CSVs returned by Circuit.

    Use this for testing on real data that can't be saved to the test suite or uploaded to Circuit.
    """
    output_paths = []
    for sheet_path in split_chunked_sheet_paths:
        with pd.ExcelFile(sheet_path) as xls:
            for sheet_name in xls.sheet_names:
                output_path = Path(f"{sheet_name}.csv")
                output_paths.append(output_path)
                df = pd.read_excel(xls, sheet_name)
                df.to_csv(output_path, index=False)

    return output_paths