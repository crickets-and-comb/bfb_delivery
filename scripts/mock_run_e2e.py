"""Mock the workflow end to end.

Use this to test on real data. in your .test_data dir.
"""

# TODO: Make a real e2e test in the test suite.
# https://github.com/crickets-and-comb/bfb_delivery/issues/103
import shutil
from pathlib import Path
from typing import Final, cast

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
    "EXTRA_NOTES_DIR": ".test_data/extra_notes",
}


@click.command()
@click.option(
    "--mock_raw_chunked_sheet_path",
    type=str,
    required=False,
    default=".test_data/reference/master_chunked.xlsx",
    help="Path to the raw chunked route sheet that this function reads in and splits up.",
)
def main(mock_raw_chunked_sheet_path: str) -> None:
    """Mock the workflow end to end.

    Use this to test on real data that can't be saved to the tests or uploaded to Circuit.

    This script takes the raw chunked workbook, splits it into workbooks to upload to Circuit,
    mocks the CSVs returned by Circuit, recombines them, and formats them.
    """
    for output_dir in OUTPUT_DIRS.values():
        shutil.rmtree(output_dir, ignore_errors=True)
        Path(output_dir).mkdir(parents=True)

    # extra_notes_file = _make_extra_notes()
    extra_notes_file = ""

    split_chunked_sheet_paths = split_chunked_route(
        input_path=mock_raw_chunked_sheet_path,
        output_dir=OUTPUT_DIRS["SPLIT_CHUNKED_DIR"],
        n_books=4,
    )
    click.echo(
        f"Split chunked route saved to: {[str(path) for path in split_chunked_sheet_paths]}"
    )

    output_paths = mock_route_tables(
        split_chunked_sheet_paths=cast(list[Path | str], split_chunked_sheet_paths),
        output_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
    )
    output_dir = Path(output_paths[0]).parent  # type: ignore[assignment]
    click.echo(f"Mocked driver route tables saved to: {output_dir}")

    combined_path = combine_route_tables(
        input_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
        output_dir=OUTPUT_DIRS["COMBINED_TABLES_DIR"],
    )
    click.echo(f"Combined workbook saved to: {combined_path}")

    formatted_path = format_combined_routes(
        input_path=combined_path,
        output_dir=OUTPUT_DIRS["FORMATTED_TABLES_DIR"],
        extra_notes_file=extra_notes_file,
    )
    click.echo(f"Formatted workbook saved to: {formatted_path}")

    final_manifests_path = create_manifests(
        input_dir=OUTPUT_DIRS["CIRCUIT_TABLES_DIR"],
        output_dir=OUTPUT_DIRS["MANIFESTS_DIR"],
        extra_notes_file=extra_notes_file,
    )
    click.echo(
        f"Manifests workbook (same as formatted workbook) saved to: {final_manifests_path}"
    )


@typechecked
def _make_extra_notes() -> str:
    """Make the extra notes file."""
    extra_notes_file_path = OUTPUT_DIRS["EXTRA_NOTES_DIR"] + "/extra_notes.xlsx"

    extra_notes_df = pd.DataFrame(
        columns=["tag", "note"],
        data=[
            (
                "Varsity Village*",
                (
                    "Varsity Village note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Tullwood Apartments*",
                (
                    "Tullwood Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Regency Park Apartments*",
                (
                    "Regency Park Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Evergreen Ridge Apartments*",
                (
                    "Evergreen Ridge Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Trailview Apartments*",
                (
                    "Trailview Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Gardenview Village*",
                (
                    "Gardenview Village note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Eleanor Apartments*",
                (
                    "Eleanor Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Walton Place*",
                (
                    "Walton Place note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Washington Square Apartments*",
                (
                    "Washington Square Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Sterling Senior Apartments*",
                (
                    "Sterling Senior Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Heart House*",
                (
                    "Heart House note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Park Ridge Apartments*",
                (
                    "Park Ridge Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Woodrose Apartments*",
                (
                    "Woodrose Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Deer Run Terrace Apartments*",
                (
                    "Deer Run Terrace Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Cascade Meadows Apartments*",
                (
                    "Cascade Meadows Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Washington Grocery Building*",
                (
                    "Washington Grocery Building note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Laurel Village*",
                (
                    "Laurel Village note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
            (
                "Laurel Forest Apartments*",
                (
                    "Laurel Forest Apartments note. "
                    "This is a dummy note. It is really long and should be so that we can "
                    "test out column width and word wrapping. It should be long enough to "
                    "wrap around to the next line. And, it should be long enough to wrap "
                    "around to the next line. And, it should be long enough to wrap around "
                    "to the next line. Hopefully, this is long enough. Also, hopefully, this "
                    "is long enough. Further, hopefully, this is long enough. Additionally, "
                    "it will help test out word wrapping merged cells."
                ),
            ),
        ],
    )

    extra_notes_df.to_csv(extra_notes_file_path, index=False)

    return extra_notes_file_path


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
