"""Unit tests for sheet_shaping.py."""

from datetime import datetime
from pathlib import Path
from typing import Final

import pandas as pd
import pytest

from bfb_delivery.lib.constants import Columns
from bfb_delivery.lib.formatting.sheet_shaping import split_chunked_route

N_BOOKS_MATRIX: Final[list[int]] = [1, 3, 4]


# TODO: Can upload multiple CSVs to Circuit instead of Excel file with multiple sheets?
class TestSplitChunkedRoute:
    """split_chunked_route splits route spreadsheet into n workbooks with sheets by driver."""

    @pytest.fixture(scope="class")
    def class_tmp_dir(self, tmp_path_factory: pytest.TempPathFactory) -> Path:
        """Get a temporary directory for the class."""
        return tmp_path_factory.mktemp("tmp")

    @pytest.fixture(scope="class")
    def mock_chunked_sheet_raw(self, class_tmp_dir: Path) -> Path:
        """Save mock chunked route sheet and get path."""
        fp: Path = class_tmp_dir / "mock_chunked_sheet_raw.xlsx"
        # TODO: Use specific sheet name.
        raw_chunked_sheet = pd.DataFrame(
            {
                Columns.DRIVER: ["A", "A", "B", "B", "C", "C", "D"],
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
        raw_chunked_sheet.to_excel(fp, index=False)

        return fp

    @pytest.mark.parametrize("output_dir_type", [Path, str])
    @pytest.mark.parametrize("output_dir", ["", "output"])
    @pytest.mark.parametrize("n_books", [1, 4])
    def test_set_output_dir(
        self,
        output_dir_type: type[Path | str],
        output_dir: Path | str,
        n_books: int,
        class_tmp_dir: Path,
        mock_chunked_sheet_raw: Path,
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = output_dir_type(class_tmp_dir / output_dir)
        output_paths = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw, output_dir=output_dir, n_books=n_books
        )
        assert all(str(output_path.parent) == str(output_dir) for output_path in output_paths)

    @pytest.mark.parametrize("output_filename", ["", "output_filename.xlsx"])
    @pytest.mark.parametrize("n_books", [1, 4])
    def test_set_output_filename(
        self, output_filename: str, mock_chunked_sheet_raw: Path, n_books: int
    ) -> None:
        """Test that the output filename can be set."""
        output_paths = split_chunked_route(
            sheet_path=mock_chunked_sheet_raw,
            output_filename=output_filename,
            n_books=n_books,
        )
        for i, output_path in enumerate(output_paths):
            expected_filename = (
                f"{output_filename.split(".")[0]}_{i + 1}.xlsx"
                if output_filename
                else f"split_workbook_{datetime.now().strftime('%Y%m%d')}_{i + 1}.xlsx"
            )
            assert output_path.name == expected_filename

    @pytest.mark.parametrize("n_books", N_BOOKS_MATRIX)
    def test_n_books_count(self, n_books: int, mock_chunked_sheet_raw: Path) -> None:
        """Test that the number of workbooks is equal to n_books."""
        output_paths = split_chunked_route(sheet_path=mock_chunked_sheet_raw, n_books=n_books)
        assert len(output_paths) == n_books

    @pytest.mark.parametrize("n_books", N_BOOKS_MATRIX)
    def test_one_driver_per_sheet(self, n_books: int, mock_chunked_sheet_raw: Path) -> None:
        """Test that each sheet contains only one driver's data."""
        output_paths = split_chunked_route(sheet_path=mock_chunked_sheet_raw, n_books=n_books)
        driver_sheets = _get_driver_sheets(output_paths=output_paths)
        assert all(sheet[Columns.DRIVER].nunique() == 1 for sheet in driver_sheets)

    @pytest.mark.parametrize("n_books", N_BOOKS_MATRIX)
    def test_sheets_named_by_driver(self, n_books: int, mock_chunked_sheet_raw: Path) -> None:
        """Test that each sheet is named after the driver."""
        output_paths = split_chunked_route(sheet_path=mock_chunked_sheet_raw, n_books=n_books)
        for output_path in output_paths:
            workbook = pd.ExcelFile(output_path)
            for sheet_name in workbook.sheet_names:
                driver_sheet = pd.read_excel(workbook, sheet_name=sheet_name)
                assert sheet_name == driver_sheet[Columns.DRIVER].unique()[0]

    @pytest.mark.parametrize("n_books", N_BOOKS_MATRIX)
    def test_unique_drivers_across_books(
        self, n_books: int, mock_chunked_sheet_raw: Path
    ) -> None:
        """Test that the drivers don't overlap between the split workbooks."""
        output_paths = split_chunked_route(sheet_path=mock_chunked_sheet_raw, n_books=n_books)

        driver_sets = []
        for output_path in output_paths:
            driver_sets.append(pd.ExcelFile(output_path).sheet_names)
        for i, driver_set in enumerate(driver_sets):
            driver_sets_sans_i = driver_sets[:i] + driver_sets[i + 1 :]  # noqa: E203
            driver_sets_sans_i = [
                driver for sublist in driver_sets_sans_i for driver in sublist
            ]
            assert len(set(driver_set).intersection(set(driver_sets_sans_i))) == 0

    @pytest.mark.parametrize("n_books", N_BOOKS_MATRIX)
    def test_complete_contents(self, n_books: int, mock_chunked_sheet_raw: Path) -> None:
        """Test that the input data is all covered in the split workbooks."""
        output_paths = split_chunked_route(sheet_path=mock_chunked_sheet_raw, n_books=n_books)

        full_data = pd.read_excel(mock_chunked_sheet_raw)

        driver_sheets = _get_driver_sheets(output_paths=output_paths)
        split_data = pd.concat(driver_sheets, ignore_index=True)

        cols = split_data.columns.to_list()
        full_data = full_data[cols].sort_values(by=cols).reset_index(drop=True)
        split_data = split_data.sort_values(by=cols).reset_index(drop=True)

        pd.testing.assert_frame_equal(full_data, split_data)

    @pytest.mark.parametrize("n_books", [0, -1])
    def test_invalid_n_books(self, n_books: int, mock_chunked_sheet_raw: Path) -> None:
        """Test that an invalid n_books raises a ValueError."""
        with pytest.raises(ValueError, match="n_books must be greater than 0."):
            _ = split_chunked_route(sheet_path=mock_chunked_sheet_raw, n_books=n_books)

    def test_invalid_n_books_driver_count(self, mock_chunked_sheet_raw: Path) -> None:
        """Test that n_books greater than the number of drivers raises a ValueError."""
        raw_sheet = pd.read_excel(mock_chunked_sheet_raw)
        driver_count = len(raw_sheet[Columns.DRIVER].unique())
        n_books = driver_count + 1
        with pytest.raises(
            ValueError,
            match=(
                f"n_books must be less than or equal to the number of drivers: "
                f"driver_count: ({driver_count}), n_books: {n_books}."
            ),
        ):
            _ = split_chunked_route(sheet_path=mock_chunked_sheet_raw, n_books=n_books)


def _get_driver_sheets(output_paths: list[Path]) -> list[pd.DataFrame]:
    driver_sheets = []
    for output_path in output_paths:
        workbook = pd.ExcelFile(output_path)
        driver_sheets = driver_sheets + [
            pd.read_excel(workbook, sheet_name=sheet) for sheet in workbook.sheet_names
        ]

    return driver_sheets
