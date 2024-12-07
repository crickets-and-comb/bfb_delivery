"""Functions for shaping and formatting spreadsheets."""

from pathlib import Path

import pandas as pd


# TODO: Allow output_dir to be specified.
# TODO: Add timestamp to file name.
# TODO: Use Pandera.
def split_chunked_route(sheet_path: Path | str) -> Path | str:
    """Split chunked route sheet into separate sheets by driver.

    Reads a spreadsheet with stops grouped by driver and splits it into separate sheets.
    Writes a new Excel workbook with each sheet containing the stops for a single driver.
    Writes adjacent to the original workbook.

    Args:
        sheet_path: Path to the chunked route sheet.

    Returns:
        Path to the split chunked route workbook.
    """
    chunked_sheet: pd.DataFrame = pd.read_excel(sheet_path)
    chunked_workbook_split_path: Path = (
        Path(sheet_path).parent / "chunked_workbook_split.xlsx"
    )
    with pd.ExcelWriter(chunked_workbook_split_path) as writer:
        for driver, data in chunked_sheet.groupby("driver"):
            data.to_excel(writer, sheet_name=str(driver), index=False)

    return chunked_workbook_split_path
