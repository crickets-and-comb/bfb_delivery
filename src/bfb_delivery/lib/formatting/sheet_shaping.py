"""Functions for shaping and formatting spreadsheets."""

from datetime import datetime
from pathlib import Path

import pandas as pd
from typeguard import typechecked


# TODO: Allow to set n sheets.
# TODO: Make column constants.
# TODO: Use Pandera.
@typechecked
def split_chunked_route(
    sheet_path: Path | str, output_dir: Path | str = "", output_filename: str = ""
) -> Path:
    """Split chunked route sheet into separate sheets by driver.

    Reads a spreadsheet with stops grouped by driver and splits it into separate sheets.
    Writes a new Excel workbook with each sheet containing the stops for a single driver.
    Writes adjacent to the original workbook.

    Args:
        sheet_path: Path to the chunked route sheet that this function reads in and splits up.
        output_dir: Directory to save the output workbook.
            Empty string (default) saves to the input `sheet_path` directory.
        output_filename: Name of the output workbook.
            Empty string (default) sets filename to "chunked_workbook_split.xlsx".

    Returns:
        Path to the split chunked route workbook.
    """
    chunked_sheet: pd.DataFrame = pd.read_excel(sheet_path)

    output_dir = Path(output_dir) if output_dir else Path(sheet_path).parent
    if output_filename == "":
        # Add date to filename.
        output_filename = f"chunked_workbook_split_{datetime.now().strftime('%Y%m%d')}.xlsx"
    output_dir.mkdir(parents=True, exist_ok=True)
    chunked_workbook_split_path: Path = output_dir / output_filename

    with pd.ExcelWriter(chunked_workbook_split_path) as writer:
        for driver, data in chunked_sheet.groupby("driver"):
            data.to_excel(writer, sheet_name=str(driver), index=False)

    return chunked_workbook_split_path
