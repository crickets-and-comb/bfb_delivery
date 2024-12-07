"""Unit tests for sheet_shaping.py."""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from bfb_delivery.lib.formatting.sheet_shaping import split_chunked_route


# TODO: Can upload multiple CSVs to Circuit instead of Excel file with multiple sheets?
# TODO: Make all these single-book tests into multibook tests?
class TestSplitChunkedRoute:
    """Test that split_chunked_route splits a spreadsheet route into sheets by driver."""

    @pytest.fixture(scope="class")
    def class_tmp_dir(self, tmp_path_factory: pytest.TempPathFactory) -> Path:
        """Get a temporary directory for the class."""
        return tmp_path_factory.mktemp("tmp")

    @pytest.fixture(scope="class")
    def mock_chunked_sheet_raw(self) -> pd.DataFrame:
        """A mock chunked route sheet."""
        # TODO: Use a fuller mock sheet with all the columns.
        return pd.DataFrame(
            {
                "driver": ["A", "A", "B", "B", "C", "C", "D"],
                "address": [
                    "123 Main",
                    "456 Elm",
                    "789 Oak",
                    "1011 Pine",
                    "1213 Maple",
                    "1415 Birch",
                    "1617 Cedar",
                ],
            }
        )

    @pytest.fixture(scope="class")
    def mock_chunked_sheet_raw_path(
        self, class_tmp_dir: Path, mock_chunked_sheet_raw: pd.DataFrame
    ) -> Path:
        """Save mock chunked route sheet and get path."""
        fp: Path = class_tmp_dir / "mock_chunked_sheet_raw.xlsx"
        # TODO: Use specific sheet name.
        mock_chunked_sheet_raw.to_excel(fp, index=False)

        return fp

    @pytest.fixture(scope="class")
    def chunked_workbook_split_single(
        self, mock_chunked_sheet_raw_path: Path
    ) -> pd.ExcelFile:
        """Get the split chunked route workbook that we are testing."""
        workbook_path = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw_path, n_books=1
        )[0]
        return pd.ExcelFile(workbook_path)

    def test_sheet_names(
        self,
        mock_chunked_sheet_raw: pd.DataFrame,
        chunked_workbook_split_single: pd.ExcelFile,
    ) -> None:
        """The sheet names in the split workbook are the unique drivers in the mock sheet."""
        driver_names = mock_chunked_sheet_raw["driver"].unique()
        assert set(chunked_workbook_split_single.sheet_names) == set(driver_names)

    @pytest.mark.parametrize("output_dir_type", [Path, str])
    @pytest.mark.parametrize("output_dir", ["", "output"])
    def test_set_output_dir(
        self,
        output_dir_type: type[Path | str],
        output_dir: Path | str,
        class_tmp_dir: Path,
        mock_chunked_sheet_raw_path: Path,
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = output_dir_type(class_tmp_dir / output_dir)
        output_path = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw_path, output_dir=output_dir, n_books=1
        )[0]
        assert str(output_path.parent) == str(output_dir)

    @pytest.mark.parametrize("output_filename", ["", "output_filename.xlsx"])
    def test_set_output_filename(
        self, output_filename: str, mock_chunked_sheet_raw_path: Path
    ) -> None:
        """Test that the output filename can be set."""
        output_path = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw_path, output_filename=output_filename, n_books=1
        )[0]
        expected_filename = (
            f"{output_filename.split(".")[0]}_1.xlsx"
            if output_filename
            else f"chunked_workbook_split_{datetime.now().strftime('%Y%m%d')}_1.xlsx"
        )
        assert output_path.name == expected_filename

    @pytest.mark.parametrize("n_books", [1, 2, 3])
    def test_n_books_count(self, n_books: int, mock_chunked_sheet_raw_path: Path) -> None:
        """Test that the number of workbooks is equal to n_books."""
        output_paths = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw_path, n_books=n_books
        )
        assert len(output_paths) == n_books

    @pytest.mark.parametrize("n_books", [1, 2, 3])
    def test_n_books_one_driver_per_sheet(
        self, n_books: int, mock_chunked_sheet_raw_path: Path
    ) -> None:
        """Test that each sheet contains only one driver's data."""
        output_paths = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw_path, n_books=n_books
        )

        driver_sheets = []
        for output_path in output_paths:
            workbook = pd.ExcelFile(output_path)
            driver_sheets = driver_sheets + [
                pd.read_excel(workbook, sheet_name=sheet) for sheet in workbook.sheet_names
            ]
        assert all(sheet["driver"].nunique() == 1 for sheet in driver_sheets)

    @pytest.mark.parametrize("n_books", [1, 2, 3])
    def test_n_books_unique_drivers(
        self, n_books: int, mock_chunked_sheet_raw_path: Path
    ) -> None:
        """Test that the drivers don't overlap between the split workbooks."""
        output_paths = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw_path, n_books=n_books
        )

        driver_sets = []
        for output_path in output_paths:
            driver_sets.append(pd.ExcelFile(output_path).sheet_names)
        for i, driver_set in enumerate(driver_sets):
            driver_sets_sans_i = driver_sets[:i] + driver_sets[i + 1 :]  # noqa: E203
            driver_sets_sans_i = [
                driver for sublist in driver_sets_sans_i for driver in sublist
            ]
            assert len(set(driver_set).intersection(set(driver_sets_sans_i))) == 0

    @pytest.mark.parametrize("n_books", [1, 2, 3])
    def test_n_books_complete_contents(
        self, n_books: int, mock_chunked_sheet_raw_path: Path
    ) -> None:
        """Test that the input data is all covered in the split workbooks."""
        output_paths = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw_path, n_books=n_books
        )

        full_data = pd.read_excel(mock_chunked_sheet_raw_path)

        driver_sheets = []
        for output_path in output_paths:
            workbook = pd.ExcelFile(output_path)
            driver_sheets = driver_sheets + [
                pd.read_excel(workbook, sheet_name=sheet) for sheet in workbook.sheet_names
            ]
        split_data = pd.concat(driver_sheets, ignore_index=True)

        cols = split_data.columns.to_list()
        full_data = full_data[cols].sort_values(by=cols).reset_index(drop=True)
        split_data = split_data.sort_values(by=cols).reset_index(drop=True)

        pd.testing.assert_frame_equal(full_data, split_data)

    @pytest.mark.parametrize("n_books", [0, -1])
    def test_invalid_n_books(self, n_books: int, mock_chunked_sheet_raw_path: Path) -> None:
        """Test that an invalid n_books raises a ValueError."""
        with pytest.raises(ValueError, match="n_books must be greater than 0."):
            _ = split_chunked_route(sheet_path=mock_chunked_sheet_raw_path, n_books=n_books)

    def test_invalid_n_books_driver_count(self, mock_chunked_sheet_raw_path: Path) -> None:
        """Test that n_books greater than the number of drivers raises a ValueError."""
        raw_sheet = pd.read_excel(mock_chunked_sheet_raw_path)
        driver_count = len(raw_sheet["driver"].unique())
        n_books = driver_count + 1
        with pytest.raises(
            ValueError,
            match=(
                f"n_books must be less than or equal to the number of drivers: "
                f"driver_count: ({driver_count}), n_books: {n_books}."
            ),
        ):
            _ = split_chunked_route(sheet_path=mock_chunked_sheet_raw_path, n_books=n_books)
