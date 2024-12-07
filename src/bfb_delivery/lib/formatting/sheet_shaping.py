"""Functions for shaping and formatting spreadsheets."""

from pathlib import Path

import pandas as pd


# TODO: Allow output_file_name to be specified.
# TODO: Add timestamp to file name.
# TODO: Use Pandera.
# TODO: Make column constants.
def split_chunked_route(sheet_path: Path | str, output_dir: Path | str = "") -> Path:
    """Split chunked route sheet into separate sheets by driver.

    Reads a spreadsheet with stops grouped by driver and splits it into separate sheets.
    Writes a new Excel workbook with each sheet containing the stops for a single driver.
    Writes adjacent to the original workbook.

    Args:
        sheet_path: Path to the chunked route sheet.
        output_dir: Directory to save the output workbook.
            Empty string (default) saves to the input `sheet_path` directory.

    Returns:
        Path to the split chunked route workbook.
    """
    chunked_sheet: pd.DataFrame = pd.read_excel(sheet_path)
    output_dir = Path(output_dir) if output_dir else Path(sheet_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    chunked_workbook_split_path: Path = output_dir / "chunked_workbook_split.xlsx"
    with pd.ExcelWriter(chunked_workbook_split_path) as writer:
        for driver, data in chunked_sheet.groupby("driver"):
            data.to_excel(writer, sheet_name=str(driver), index=False)

    return chunked_workbook_split_path
