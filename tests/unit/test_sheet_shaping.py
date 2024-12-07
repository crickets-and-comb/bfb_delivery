from pathlib import Path

import pandas as pd
import pytest

from bfb_delivery.lib.formatting.sheet_shaping import split_chunked_route


# TODO: Can you upload multiple CSVs to Circuit instead of a single Excel file with multiple sheets?
class TestSplitChunkedRoute:
    """Test that split_chunked_route splits a spreadsheet route into sheets by driver."""

    @pytest.fixture(scope="class")
    def mock_chunked_sheet_raw(self) -> pd.DataFrame:
        """A mock chunked route sheet."""
        # TODO: Use a fuller mock sheet with all the columns and data types to ensure that the function works as expected.
        return pd.DataFrame(
            {
                "driver": ["A", "A", "B", "B", "C", "C"],
                "address": [
                    "123 Main",
                    "456 Elm",
                    "789 Oak",
                    "1011 Pine",
                    "1213 Maple",
                    "1415 Birch",
                ],
            }
        )

    @pytest.fixture(scope="class")
    def mock_chunked_sheet_raw_path(
        self, tmp_path: Path, mock_chunked_sheet_raw: pd.DataFrame
    ) -> Path:
        """Save mock chunked route sheet and get path."""
        fp: Path = tmp_path / "mock_chunked_sheet_raw.xlsx"
        # TODO: Use specific sheet name.
        mock_chunked_sheet_raw.to_excel(fp, index=False)

        return fp

    @pytest.fixture(scope="class")
    def mock_chunked_workbook_split_path(
        self, tmp_path: Path, mock_chunked_sheet_raw: pd.DataFrame
    ) -> Path:
        """A mocked chunked route workbook, split into sheets by driver."""
        # For each driver, create a sheet with the driver's data.
        chunked_workbook_split_path: Path = tmp_path / "mock_chunked_workbook_split.xlsx"
        with pd.ExcelWriter(chunked_workbook_split_path) as writer:
            for driver, data in mock_chunked_sheet_raw.groupby("driver"):
                data.to_excel(writer, sheet_name=str(driver), index=False)

        return chunked_workbook_split_path

    @pytest.fixture(scope="class")
    def chunked_workbook_split_path(self, mock_chunked_sheet_raw_path: Path) -> Path | str:
        """Get the path to the split chunked route workbook that we are testing."""
        return split_chunked_route(sheet_path=mock_chunked_sheet_raw_path)

    @pytest.fixture(scope="class")
    def chunked_workbook_split(self, chunked_workbook_split_path: Path | str) -> pd.ExcelFile:
        """Get the split chunked route workbook that we are testing."""
        return pd.ExcelFile(chunked_workbook_split_path)

    def test_by_oracle(
        self,
        mock_chunked_workbook_split_path: Path,
        chunked_workbook_split: pd.ExcelFile,
        chunked_workbook_split_path: Path | str,
    ) -> None:
        """Test that split_chunked_route matches oracle."""
        for sheet_name in chunked_workbook_split.sheet_names:
            test_chunked_sheet = pd.read_excel(
                mock_chunked_workbook_split_path, sheet_name=sheet_name
            )
            result_chunked_sheet = pd.read_excel(
                chunked_workbook_split_path, sheet_name=sheet_name
            )
            pd.testing.assert_frame_equal(test_chunked_sheet, result_chunked_sheet)

    def test_driver_count(
        self, mock_chunked_sheet_raw: pd.DataFrame, chunked_workbook_split: pd.ExcelFile
    ) -> None:
        """The number of sheets in the split workbook is equal to the number of drivers in the mock sheet."""
        # TODO: Make columns constants.
        driver_count = mock_chunked_sheet_raw["driver"].nunique()
        assert len(chunked_workbook_split.sheet_names) == driver_count

    def test_sheet_names(
        self, mock_chunked_sheet_raw: pd.DataFrame, chunked_workbook_split: pd.ExcelFile
    ) -> None:
        """The sheet names in the split workbook are the unique drivers in the mock sheet."""
        driver_names = mock_chunked_sheet_raw["driver"].unique()
        assert set(chunked_workbook_split.sheet_names) == set(driver_names)
