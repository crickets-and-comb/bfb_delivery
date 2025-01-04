"""Unit tests for sheet_shaping.py."""

import re
import subprocess
from collections.abc import Iterator
from contextlib import AbstractContextManager, nullcontext
from datetime import datetime
from pathlib import Path
from typing import Final

import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook

from bfb_delivery import (
    combine_route_tables,
    create_manifests,
    format_combined_routes,
    split_chunked_route,
)
from bfb_delivery.lib.constants import (
    BOX_TYPE_COLOR_MAP,
    COMBINED_ROUTES_COLUMNS,
    FILE_DATE_FORMAT,
    FORMATTED_ROUTES_COLUMNS,
    MANIFEST_DATE_FORMAT,
    NOTES_COLUMN_WIDTH,
    SPLIT_ROUTE_COLUMNS,
    BoxType,
    CellColors,
    Columns,
)
from bfb_delivery.lib.formatting.data_cleaning import (
    _format_and_validate_box_type,
    _format_and_validate_name,
    _format_and_validate_neighborhood,
    _format_and_validate_phone,
)
from bfb_delivery.lib.formatting.sheet_shaping import _aggregate_route_data

N_BOOKS_MATRIX: Final[list[int]] = [1, 3, 4]
DRIVERS: Final[list[str]] = ["Driver One", "Driver Two", "Driver Three", "Driver Four"]
BOX_TYPES: Final[list[str]] = ["Basic", "GF", "Vegan", "LA"]
MANIFEST_DATE: Final[str] = "1.1"
NEIGHBORHOODS: Final[list[str]] = ["York", "Puget", "Samish", "Sehome", "South Hill"]


@pytest.fixture(scope="module")
def module_tmp_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Get a temporary directory for the class."""
    return tmp_path_factory.mktemp("tmp")


@pytest.fixture(scope="module")
def mock_chunked_sheet_raw(module_tmp_dir: Path) -> Path:
    """Save mock chunked route sheet and get path."""
    fp: Path = module_tmp_dir / "mock_chunked_sheet_raw.xlsx"
    raw_chunked_sheet = pd.DataFrame(
        columns=SPLIT_ROUTE_COLUMNS + [Columns.DRIVER, Columns.BOX_COUNT, Columns.STOP_NO],
        data=[
            (
                "Recipient One",
                "123 Main St",
                "555-555-1234",
                "Recipient1@email.com",
                "Notes for Recipient One.",
                "1",
                "Basic",
                "York",
                "Driver One",
                2,
                1,
            ),
            (
                "Recipient Two",
                "456 Elm St",
                "555-555-5678",
                "Recipient2@email.com",
                "Notes for Recipient Two.",
                "1",
                "GF",
                "Puget",
                "Driver One",
                None,
                2,
            ),
            (
                "Recipient Three",
                "789 Oak St",
                "555-555-9101",
                "Recipient3@email.com",
                "Notes for Recipient Three.",
                "1",
                "Vegan",
                "Puget",
                "Driver Two",
                2,
                3,
            ),
            (
                "Recipient Four",
                "1011 Pine St",
                "555-555-1121",
                "Recipient4@email.com",
                "Notes for Recipient Four.",
                "1",
                "LA",
                "Puget",
                "Driver Two",
                None,
                4,
            ),
            (
                "Recipient Five",
                "1314 Cedar St",
                "555-555-3141",
                "Recipient5@email.com",
                "Notes for Recipient Five.",
                "1",
                "Basic",
                "Samish",
                "Driver Three",
                2,
                5,
            ),
            (
                "Recipient Six",
                "1516 Fir St",
                "555-555-5161",
                "Recipient6@email.com",
                "Notes for Recipient Six.",
                "1",
                "GF",
                "Sehome",
                "Driver Three",
                None,
                6,
            ),
            (
                "Recipient Seven",
                "1718 Spruce St",
                "555-555-7181",
                "Recipient7@email.com",
                "Notes for Recipient Seven.",
                "1",
                "Vegan",
                "Samish",
                "Driver Three",
                None,
                7,
            ),
            (
                "Recipient Eight",
                "1920 Maple St",
                "555-555-9202",
                "Recipient8@email.com",
                "Notes for Recipient Eight.",
                "1",
                "LA",
                "South Hill",
                "Driver Four",
                1,
                8,
            ),
        ],
    ).rename(columns={Columns.PRODUCT_TYPE: Columns.BOX_TYPE})
    raw_chunked_sheet.to_excel(fp, index=False)

    return fp


@pytest.fixture()
def mock_route_tables(tmp_path: Path, mock_chunked_sheet_raw: Path) -> Path:
    """Mock the driver route tables returned by Circuit."""
    output_dir = tmp_path / "mock_route_tables"
    output_dir.mkdir()

    output_cols = [Columns.STOP_NO] + SPLIT_ROUTE_COLUMNS
    chunked_df = pd.read_excel(mock_chunked_sheet_raw)
    chunked_df.rename(columns={Columns.BOX_TYPE: Columns.PRODUCT_TYPE}, inplace=True)
    for driver in chunked_df[Columns.DRIVER].unique():
        output_path = output_dir / f"{driver}.csv"
        driver_df = chunked_df[chunked_df[Columns.DRIVER] == driver]
        driver_df[Columns.STOP_NO] = [i + 1 for i in range(len(driver_df))]
        driver_df[output_cols].to_csv(output_dir / output_path, index=False)

    return output_dir


@pytest.fixture()
def mock_combined_routes(module_tmp_dir: Path) -> Path:
    """Mock the combined routes table."""
    output_path = module_tmp_dir / "combined_routes.xlsx"
    with pd.ExcelWriter(output_path) as writer:
        for driver in DRIVERS:
            df = pd.DataFrame(columns=COMBINED_ROUTES_COLUMNS)
            stops = [stop_no + 1 for stop_no in range(9)]
            df[Columns.STOP_NO] = stops
            df[Columns.NAME] = [f"{driver} Recipient {stop_no}" for stop_no in stops]
            df[Columns.ADDRESS] = [f"{driver} stop {stop_no} address" for stop_no in stops]
            df[Columns.PHONE] = ["13607345215"] * len(stops)
            df[Columns.NOTES] = [f"{driver} stop {stop_no} notes" for stop_no in stops]
            df[Columns.ORDER_COUNT] = [1] * len(stops)
            df[Columns.BOX_TYPE] = [BOX_TYPES[i % len(BOX_TYPES)] for i in range(len(stops))]
            df[Columns.NEIGHBORHOOD] = [
                NEIGHBORHOODS[i % len(NEIGHBORHOODS)] for i in range(len(stops))
            ]

            assert df.isna().sum().sum() == 0
            assert set(df.columns.to_list()) == set(COMBINED_ROUTES_COLUMNS)

            df.to_excel(writer, sheet_name=driver, index=False)

    return output_path


@pytest.fixture()
def mock_combined_routes_ExcelFile(mock_combined_routes: Path) -> Iterator[pd.ExcelFile]:
    """Mock the combined routes table ExcelFile."""
    with pd.ExcelFile(mock_combined_routes) as xls:
        yield xls


# TODO: Can upload multiple CSVs to Circuit instead of Excel file with multiple sheets?
@pytest.mark.usefixtures("mock_is_valid_number")
class TestSplitChunkedRoute:
    """split_chunked_route splits route spreadsheet into n workbooks with sheets by driver."""

    @pytest.mark.parametrize("output_dir_type", [Path, str])
    @pytest.mark.parametrize("output_dir", ["", "dummy_output"])
    @pytest.mark.parametrize("n_books", [1, 4])
    def test_set_output_dir(
        self,
        output_dir_type: type[Path | str],
        output_dir: Path | str,
        n_books: int,
        module_tmp_dir: Path,
        mock_chunked_sheet_raw: Path,
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = output_dir_type(module_tmp_dir / output_dir)
        output_paths = split_chunked_route(
            input_path=mock_chunked_sheet_raw, output_dir=output_dir, n_books=n_books
        )
        assert all(str(output_path.parent) == str(output_dir) for output_path in output_paths)

    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.xlsx"])
    @pytest.mark.parametrize("n_books", [1, 4])
    def test_set_output_filename(
        self, output_filename: str, mock_chunked_sheet_raw: Path, n_books: int
    ) -> None:
        """Test that the output filename can be set."""
        output_paths = split_chunked_route(
            input_path=mock_chunked_sheet_raw,
            output_filename=output_filename,
            n_books=n_books,
        )
        for i, output_path in enumerate(output_paths):
            expected_filename = output_filename.split(".")[0]
            expected_filename = (
                f"{expected_filename}_{i + 1}.xlsx"
                if output_filename
                else (
                    f"split_workbook_{datetime.now().strftime(FILE_DATE_FORMAT)}"
                    f"_{i + 1}.xlsx"
                )
            )
            assert output_path.name == expected_filename

    @pytest.mark.parametrize("n_books_passed", N_BOOKS_MATRIX + [None])
    def test_n_books_count(
        self, n_books_passed: int | None, mock_chunked_sheet_raw: Path
    ) -> None:
        """Test that the number of workbooks is equal to n_books."""
        if n_books_passed is None:
            output_paths = split_chunked_route(input_path=mock_chunked_sheet_raw)
            n_books = 4
        else:
            n_books = n_books_passed
            output_paths = split_chunked_route(
                input_path=mock_chunked_sheet_raw, n_books=n_books
            )

        assert len(output_paths) == n_books

    @pytest.mark.parametrize("n_books", N_BOOKS_MATRIX)
    def test_recipients_unique(self, n_books: int, mock_chunked_sheet_raw: Path) -> None:
        """Test that the recipients don't overlap between the split workbooks.

        By name, address, phone, and email.
        """
        output_paths = split_chunked_route(input_path=mock_chunked_sheet_raw, n_books=n_books)

        recipient_sets = []
        for output_path in output_paths:
            driver_sheets = _get_driver_sheets(output_paths=[output_path])
            recipient_sets.append(
                pd.concat(driver_sheets, ignore_index=True)[
                    [Columns.NAME, Columns.ADDRESS, Columns.PHONE, Columns.EMAIL]
                ]
            )
        recipients_df = pd.concat(recipient_sets, ignore_index=True)
        assert recipients_df.duplicated().sum() == 0

    @pytest.mark.parametrize("n_books", N_BOOKS_MATRIX)
    def test_unique_drivers_across_books(
        self, n_books: int, mock_chunked_sheet_raw: Path
    ) -> None:
        """Test that the drivers don't overlap between the split workbooks."""
        output_paths = split_chunked_route(input_path=mock_chunked_sheet_raw, n_books=n_books)

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
        output_paths = split_chunked_route(input_path=mock_chunked_sheet_raw, n_books=n_books)

        full_data = pd.read_excel(mock_chunked_sheet_raw)

        driver_sheets = _get_driver_sheets(output_paths=output_paths)
        split_data = pd.concat(driver_sheets, ignore_index=True)

        split_data.rename(columns={Columns.PRODUCT_TYPE: Columns.BOX_TYPE}, inplace=True)
        cols = split_data.columns.to_list()
        full_data = full_data[cols].sort_values(by=cols).reset_index(drop=True)
        split_data = split_data.sort_values(by=cols).reset_index(drop=True)

        # Hacky, but need to make sure formatted values haven't fundamentally changed.
        cols_without_formatting = [
            col
            for col in cols
            if col
            not in [Columns.PHONE, Columns.NAME, Columns.BOX_TYPE, Columns.NEIGHBORHOOD]
        ]
        pd.testing.assert_frame_equal(
            full_data[cols_without_formatting], split_data[cols_without_formatting]
        )

        phone_df = full_data[[Columns.PHONE]].copy()
        _format_and_validate_phone(df=phone_df)
        assert phone_df[Columns.PHONE].equals(split_data[Columns.PHONE])

        name_df = full_data[[Columns.NAME]].copy()
        _format_and_validate_name(df=name_df)
        assert name_df[Columns.NAME].equals(split_data[Columns.NAME])

        box_type_df = full_data[[Columns.BOX_TYPE]].copy()
        _format_and_validate_box_type(df=box_type_df)
        assert box_type_df[Columns.BOX_TYPE].equals(split_data[Columns.BOX_TYPE])

        neighborhood_df = full_data[[Columns.NEIGHBORHOOD]].copy()
        _format_and_validate_neighborhood(df=neighborhood_df)
        assert neighborhood_df[Columns.NEIGHBORHOOD].equals(split_data[Columns.NEIGHBORHOOD])

    @pytest.mark.parametrize("n_books", [0, -1])
    def test_invalid_n_books(self, n_books: int, mock_chunked_sheet_raw: Path) -> None:
        """Test that an invalid n_books raises a ValueError."""
        with pytest.raises(ValueError, match="n_books must be greater than 0."):
            _ = split_chunked_route(input_path=mock_chunked_sheet_raw, n_books=n_books)

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
            _ = split_chunked_route(input_path=mock_chunked_sheet_raw, n_books=n_books)

    @pytest.mark.parametrize(
        "output_dir, output_filename, n_books",
        [("", "", 4), ("output", "", 3), ("", "output_filename.xlsx", 1)],
    )
    def test_cli(
        self,
        output_dir: str,
        output_filename: str,
        n_books: int,
        mock_chunked_sheet_raw: Path,
        module_tmp_dir: Path,
    ) -> None:
        """Test CLI works."""
        output_dir = str(module_tmp_dir / output_dir) if output_dir else output_dir
        arg_list = [
            "--input_path",
            str(mock_chunked_sheet_raw),
            "--output_dir",
            output_dir,
            "--output_filename",
            output_filename,
            "--n_books",
            str(n_books),
        ]

        result = subprocess.run(["split_chunked_route"] + arg_list, capture_output=True)
        assert result.returncode == 0

        for i in range(n_books):
            expected_filename = (
                f"{output_filename.split('.')[0]}_{i + 1}.xlsx"
                if output_filename
                else (
                    f"split_workbook_{datetime.now().strftime(FILE_DATE_FORMAT)}"
                    f"_{i + 1}.xlsx"
                )
            )
            expected_output_dir = (
                Path(output_dir) if output_dir else mock_chunked_sheet_raw.parent
            )
            assert (Path(expected_output_dir) / expected_filename).exists()

    def test_output_columns(self, mock_chunked_sheet_raw: Path) -> None:
        """Test that the output columns match the SPLIT_ROUTE_COLUMNS constant."""
        output_paths = split_chunked_route(input_path=mock_chunked_sheet_raw)
        for output_path in output_paths:
            workbook = pd.ExcelFile(output_path)
            for sheet_name in workbook.sheet_names:
                driver_sheet = pd.read_excel(workbook, sheet_name=sheet_name)
                assert driver_sheet.columns.to_list() == SPLIT_ROUTE_COLUMNS


class TestCombineRouteTables:
    """combine_route_tables combines driver route CSVs into a single workbook."""

    @pytest.fixture()
    def basic_combined_routes(self, mock_route_tables: Path) -> Path:
        """Create a basic combined routes table scoped to class for reuse."""
        output_dir = mock_route_tables.parent / "basic_combined_routes"
        output_dir.mkdir()
        output_path = combine_route_tables(input_dir=mock_route_tables, output_dir=output_dir)
        return output_path

    @pytest.mark.parametrize("output_dir_type", [Path, str])
    @pytest.mark.parametrize("output_dir", ["", "dummy_output"])
    def test_set_output_dir(
        self,
        output_dir_type: type[Path | str],
        output_dir: Path | str,
        module_tmp_dir: Path,
        mock_route_tables: Path,
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = output_dir_type(module_tmp_dir / output_dir)
        output_path = combine_route_tables(input_dir=mock_route_tables, output_dir=output_dir)
        assert str(output_path.parent) == str(output_dir)

    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.csv"])
    def test_set_output_filename(self, output_filename: str, mock_route_tables: Path) -> None:
        """Test that the output filename can be set."""
        output_path = combine_route_tables(
            input_dir=mock_route_tables, output_filename=output_filename
        )
        expected_filename = (
            f"combined_routes_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
            if output_filename == ""
            else output_filename
        )
        assert output_path.name == expected_filename

    def test_output_columns(self, basic_combined_routes: Path) -> None:
        """Test that the output columns match the COMBINED_ROUTES_COLUMNS constant."""
        workbook = pd.ExcelFile(basic_combined_routes)
        for sheet_name in workbook.sheet_names:
            driver_sheet = pd.read_excel(workbook, sheet_name=sheet_name)
            assert driver_sheet.columns.to_list() == COMBINED_ROUTES_COLUMNS

    def test_unique_recipients(self, basic_combined_routes: Path) -> None:
        """Test that the recipients don't overlap between the driver route tables.

        By name, address, and phone.
        """
        driver_sheets = _get_driver_sheets(output_paths=[basic_combined_routes])
        combined_output_data = pd.concat(driver_sheets, ignore_index=True)
        assert (
            combined_output_data[[Columns.NAME, Columns.ADDRESS, Columns.PHONE]]
            .duplicated()
            .sum()
            == 0  # noqa: W503
        )

    def test_complete_contents(
        self, mock_route_tables: Path, basic_combined_routes: Path
    ) -> None:
        """Test that the input data is all covered in the combined workbook."""
        mock_table_paths = list(mock_route_tables.glob("*"))
        full_input_data = pd.concat(
            [pd.read_csv(path) for path in mock_table_paths], ignore_index=True
        ).rename(columns={Columns.PRODUCT_TYPE: Columns.BOX_TYPE})[COMBINED_ROUTES_COLUMNS]
        driver_sheets = _get_driver_sheets(output_paths=[basic_combined_routes])
        combined_output_data = pd.concat(driver_sheets, ignore_index=True)

        full_input_data = full_input_data.sort_values(by=COMBINED_ROUTES_COLUMNS).reset_index(
            drop=True
        )
        combined_output_data = combined_output_data.sort_values(
            by=COMBINED_ROUTES_COLUMNS
        ).reset_index(drop=True)

        pd.testing.assert_frame_equal(full_input_data, combined_output_data)

    @pytest.mark.parametrize("output_dir", ["dummy_output", ""])
    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.xlsx"])
    def test_cli(
        self,
        output_dir: str,
        output_filename: str,
        mock_route_tables: Path,
        module_tmp_dir: Path,
    ) -> None:
        """Test CLI works."""
        output_dir = str(module_tmp_dir / output_dir) if output_dir else output_dir
        arg_list = [
            "--input_dir",
            str(mock_route_tables),
            "--output_dir",
            output_dir,
            "--output_filename",
            output_filename,
        ]

        result = subprocess.run(["combine_route_tables"] + arg_list, capture_output=True)
        assert result.returncode == 0

        expected_output_filename = (
            f"combined_routes_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
            if output_filename == ""
            else output_filename
        )
        expected_output_dir = Path(output_dir) if output_dir else mock_route_tables
        assert (expected_output_dir / expected_output_filename).exists()


class TestFormatCombinedRoutes:
    """format_combined_routes formats the combined routes table."""

    @pytest.fixture()
    def basic_manifest(self, mock_combined_routes: Path) -> Path:
        """Create a basic manifest scoped to class for reuse."""
        output_path = format_combined_routes(
            input_path=mock_combined_routes, date=MANIFEST_DATE
        )
        return output_path

    @pytest.fixture()
    def basic_manifest_workbook(self, basic_manifest: Path) -> Workbook:
        """Create a basic manifest workbook scoped to class for reuse."""
        workbook = load_workbook(basic_manifest)
        return workbook

    @pytest.mark.parametrize("output_dir_type", [Path, str])
    @pytest.mark.parametrize("output_dir", ["", "dummy_output"])
    def test_set_output_dir(
        self,
        output_dir_type: type[Path | str],
        output_dir: Path | str,
        module_tmp_dir: Path,
        mock_combined_routes: Path,
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = output_dir_type(module_tmp_dir / output_dir)
        output_path = format_combined_routes(
            input_path=mock_combined_routes, output_dir=output_dir
        )
        assert str(output_path.parent) == str(output_dir)

    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.csv"])
    def test_set_output_filename(
        self, output_filename: str, mock_combined_routes: Path
    ) -> None:
        """Test that the output filename can be set."""
        output_path = format_combined_routes(
            input_path=mock_combined_routes, output_filename=output_filename
        )
        expected_output_filename = (
            f"formatted_routes_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
            if output_filename == ""
            else output_filename
        )
        assert output_path.name == expected_output_filename

    @pytest.mark.parametrize(
        "date, expected_date",
        [("", datetime.now().strftime(MANIFEST_DATE_FORMAT)), ("Dummy date", "Dummy date")],
    )
    def test_all_drivers_have_a_sheet(
        self, mock_combined_routes: Path, date: str | None, expected_date: str
    ) -> None:
        """Test that all drivers have a sheet in the formatted workbook. And date works."""
        sheet_names = set([f"{expected_date} {driver}" for driver in DRIVERS])
        kwargs: dict[str, str] = {"input_path": str(mock_combined_routes)}
        if date is not None:
            kwargs["date"] = str(date)

        output_path = format_combined_routes(**kwargs)
        workbook = pd.ExcelFile(output_path)
        assert set(workbook.sheet_names) == sheet_names

    @pytest.mark.parametrize("output_dir", ["dummy_output", ""])
    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.xlsx"])
    def test_cli(
        self,
        output_dir: str,
        output_filename: str,
        mock_combined_routes: Path,
        module_tmp_dir: Path,
    ) -> None:
        """Test CLI works."""
        output_dir = str(module_tmp_dir / output_dir) if output_dir else output_dir
        arg_list = [
            "--input_path",
            str(mock_combined_routes),
            "--output_dir",
            output_dir,
            "--output_filename",
            output_filename,
        ]

        result = subprocess.run(["format_combined_routes"] + arg_list, capture_output=True)
        assert result.returncode == 0

        expected_output_filename = (
            f"formatted_routes_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
            if output_filename == ""
            else output_filename
        )
        expected_output_dir = Path(output_dir) if output_dir else mock_combined_routes.parent
        assert (expected_output_dir / expected_output_filename).exists()

    def test_df_is_same(
        self, mock_combined_routes_ExcelFile: pd.ExcelFile, basic_manifest: Path
    ) -> None:
        """All the input data is in the formatted workbook."""
        for sheet_name in sorted(mock_combined_routes_ExcelFile.sheet_names):
            input_df = pd.read_excel(mock_combined_routes_ExcelFile, sheet_name=sheet_name)
            input_df.sort_values(by=[Columns.STOP_NO], inplace=True)
            output_df = pd.read_excel(
                basic_manifest, sheet_name=f"{MANIFEST_DATE} {sheet_name}", skiprows=8
            )

            # Hacky, but need to make sure formatted values haven't fundamentally changed.
            formatted_columns = [Columns.BOX_TYPE, Columns.NAME, Columns.PHONE]
            unformatted_columns = [
                col for col in FORMATTED_ROUTES_COLUMNS if col not in formatted_columns
            ]
            assert input_df[unformatted_columns].equals(output_df[unformatted_columns])

            input_box_type_df = input_df[[Columns.BOX_TYPE]]
            _format_and_validate_box_type(df=input_box_type_df)
            assert input_box_type_df.equals(output_df[[Columns.BOX_TYPE]])

            input_name_df = input_df[[Columns.NAME]]
            _format_and_validate_name(df=input_name_df)
            assert input_name_df.equals(output_df[[Columns.NAME]])

            input_phone_df = input_df[[Columns.PHONE]]
            _format_and_validate_phone(df=input_phone_df)
            assert input_phone_df.equals(output_df[[Columns.PHONE]])

    @pytest.mark.parametrize(
        "cell, expected_value",
        [
            ("A1", "DRIVER SUPPORT: 555-555-5555"),
            ("B1", None),
            ("C1", None),
            ("D1", "RECIPIENT SUPPORT: 555-555-5555 x5"),
            ("E1", None),
            ("F1", "PLEASE SHRED MANIFEST AFTER COMPLETING ROUTE."),
        ],
    )
    def test_header_row(
        self, cell: str, expected_value: str, basic_manifest_workbook: Workbook
    ) -> None:
        """Test that the header row is correct."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws[cell].value == expected_value

    def test_header_row_end(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the header row ends at F1."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            last_non_empty_col = max(
                (cell.column for cell in ws[1] if cell.value), default=None
            )
            assert last_non_empty_col == 6

    @pytest.mark.parametrize("cell", ["A1", "B1", "C1", "D1", "E1", "F1"])
    def test_header_row_color(self, cell: str, basic_manifest_workbook: Workbook) -> None:
        """Test the header row fill color."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws[cell].fill.start_color.rgb == f"{CellColors.HEADER}"

    def test_date_cell(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the date cell is correct."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws["A3"].value == f"Date: {MANIFEST_DATE}"

    def test_driver_cell(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the driver cell is correct."""
        drivers = [driver.upper() for driver in DRIVERS]
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            driver_name = sheet_name.replace(f"{MANIFEST_DATE} ", "")
            assert ws["A5"].value == f"Driver: {driver_name}"
            assert driver_name.upper() in drivers

    def test_agg_cells(
        self,
        mock_combined_routes_ExcelFile: pd.ExcelFile,
        basic_manifest_workbook: Workbook,  # noqa: E501
    ) -> None:
        """Test that the aggregated cells are correct."""
        for sheet_name in sorted(mock_combined_routes_ExcelFile.sheet_names):
            input_df = pd.read_excel(mock_combined_routes_ExcelFile, sheet_name=sheet_name)
            manifest_sheet_name = f"{MANIFEST_DATE} {sheet_name}"
            ws = basic_manifest_workbook[manifest_sheet_name]

            agg_dict = _aggregate_route_data(df=input_df)

            neighborhoods = ", ".join(agg_dict["neighborhoods"])
            assert ws["A7"].value == f"Neighborhoods: {neighborhoods.upper()}"
            assert ws["E3"].value == BoxType.BASIC
            assert ws["F3"].value == agg_dict["box_counts"][BoxType.BASIC]
            assert ws["E4"].value == BoxType.GF
            assert ws["F4"].value == agg_dict["box_counts"][BoxType.GF]
            assert ws["E5"].value == BoxType.LA
            assert ws["F5"].value == agg_dict["box_counts"][BoxType.LA]
            assert ws["E6"].value == BoxType.VEGAN
            assert ws["F6"].value == agg_dict["box_counts"][BoxType.VEGAN]
            assert ws["E7"].value == "TOTAL BOX COUNT="
            assert ws["F7"].value == agg_dict["total_box_count"]
            assert ws["E8"].value == "PROTEIN COUNT="
            assert ws["F8"].value == agg_dict["protein_box_count"]

    def test_box_type_cell_colors(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the box type cells conditionally formatted with fill color."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            for cell in ws["F"]:
                if cell.row > 9:
                    assert cell.fill.start_color.rgb == f"{BOX_TYPE_COLOR_MAP[cell.value]}"
            for cell in ws["E"]:
                if cell.row > 2 and cell.row < 7:
                    assert cell.fill.start_color.rgb == f"{BOX_TYPE_COLOR_MAP[cell.value]}"

    def test_notes_column_width(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the notes column width is correct."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws["E9"].value == Columns.NOTES
            assert ws.column_dimensions["E"].width == NOTES_COLUMN_WIDTH

    @pytest.mark.parametrize(
        "cell",
        [
            # Header row.
            "A1",
            "B1",
            "C1",
            "D1",
            "E1",
            "F1",
            # Aggregated data.
            "A3",
            "A5",
            "A7",
            "E3",
            "E4",
            "E5",
            "E6",
            "E7",
            "E8",
            "F3",
            "F4",
            "F5",
            "F6",
            "F7",
            "F8",
            # Data header.
            "A9",
            "B9",
            "C9",
            "D9",
            "E9",
            "F9",
        ],
    )
    def test_bold_cells(self, cell: str, basic_manifest_workbook: Workbook) -> None:
        """Test that the cells are bold."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws[cell].font.bold

    def test_cell_right_alignment(self, basic_manifest_workbook: Workbook) -> None:
        """Test right-aligned cells."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            right_aligned_cells = [ws["D1"], ws["F1"]] + [
                cell for row in ws["E3:F8"] for cell in row
            ]
            for cell in right_aligned_cells:
                assert cell.alignment.horizontal == "right"

    def test_cell_left_alignment(self, basic_manifest_workbook: Workbook) -> None:
        """Test left-aligned cells."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            left_aligned_cells = [cell for row in ws["A1:A8"] for cell in row] + [
                cell for row in ws["A9:F9"] for cell in row
            ]
            for cell in left_aligned_cells:
                assert cell.alignment.horizontal == "left"


class TestCreateManifests:
    """create_manifests formats the route tables CSVs."""

    @pytest.fixture()
    def basic_manifest(self, mock_route_tables: Path) -> Path:
        """Create a basic manifest scoped to class for reuse."""
        output_path = create_manifests(input_dir=mock_route_tables, date=MANIFEST_DATE)
        return output_path

    @pytest.fixture()
    def basic_manifest_workbook(self, basic_manifest: Path) -> Workbook:
        """Create a basic manifest workbook scoped to class for reuse."""
        workbook = load_workbook(basic_manifest)
        return workbook

    @pytest.fixture()
    def basic_manifest_ExcelFile(self, basic_manifest: Path) -> Iterator[pd.ExcelFile]:
        """Create a basic manifest workbook scoped to class for reuse."""
        with pd.ExcelFile(basic_manifest) as xls:
            yield xls

    @pytest.mark.parametrize("output_dir_type", [Path, str])
    @pytest.mark.parametrize("output_dir", ["", "dummy_output"])
    def test_set_output_dir(
        self,
        output_dir_type: type[Path | str],
        output_dir: Path | str,
        module_tmp_dir: Path,
        mock_route_tables: Path,
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = output_dir_type(module_tmp_dir / output_dir)
        output_path = create_manifests(input_dir=mock_route_tables, output_dir=output_dir)
        assert str(output_path.parent) == str(output_dir)

    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.csv"])
    def test_set_output_filename(self, output_filename: str, mock_route_tables: Path) -> None:
        """Test that the output filename can be set."""
        output_path = create_manifests(
            input_dir=mock_route_tables, output_filename=output_filename
        )
        expected_output_filename = (
            f"final_manifests_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
            if output_filename == ""
            else output_filename
        )
        assert output_path.name == expected_output_filename

    @pytest.mark.parametrize(
        "date, expected_date",
        [("", datetime.now().strftime(MANIFEST_DATE_FORMAT)), ("Dummy date", "Dummy date")],
    )
    def test_all_drivers_have_a_sheet(
        self, mock_route_tables: Path, date: str | None, expected_date: str
    ) -> None:
        """Test that all drivers have a sheet in the formatted workbook. And date works."""
        sheet_names = set([f"{expected_date} {driver}" for driver in DRIVERS])
        kwargs: dict[str, str] = {"input_dir": str(mock_route_tables)}
        if date is not None:
            kwargs["date"] = str(date)

        output_path = create_manifests(**kwargs)
        workbook = pd.ExcelFile(output_path)
        assert set(workbook.sheet_names) == sheet_names

    @pytest.mark.parametrize("output_dir", ["dummy_output", ""])
    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.xlsx"])
    def test_cli(
        self,
        output_dir: str,
        output_filename: str,
        mock_route_tables: Path,
        module_tmp_dir: Path,
    ) -> None:
        """Test CLI works."""
        output_dir = str(module_tmp_dir / output_dir) if output_dir else output_dir
        arg_list = [
            "--input_dir",
            str(mock_route_tables),
            "--output_dir",
            output_dir,
            "--output_filename",
            output_filename,
        ]

        result = subprocess.run(["create_manifests"] + arg_list, capture_output=True)
        assert result.returncode == 0

        expected_output_filename = (
            f"final_manifests_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
            if output_filename == ""
            else output_filename
        )
        expected_output_dir = Path(output_dir) if output_dir else mock_route_tables
        assert (expected_output_dir / expected_output_filename).exists()

    def test_df_is_same(
        self, mock_route_tables: Path, basic_manifest_ExcelFile: pd.ExcelFile
    ) -> None:
        """All the input data is in the formatted workbook."""
        for sheet_name in sorted(basic_manifest_ExcelFile.sheet_names):
            driver = str(sheet_name).replace(f"{MANIFEST_DATE} ", "")
            input_df = pd.read_csv(mock_route_tables / f"{driver}.csv")
            output_df = pd.read_excel(
                basic_manifest_ExcelFile, sheet_name=sheet_name, skiprows=8
            )

            # Hacky, but need to make sure formatted values haven't fundamentally changed.
            formatted_columns = [Columns.BOX_TYPE, Columns.NAME, Columns.PHONE]
            unformatted_columns = [
                col for col in FORMATTED_ROUTES_COLUMNS if col not in formatted_columns
            ]
            assert input_df[unformatted_columns].equals(output_df[unformatted_columns])

            input_df.rename(columns={Columns.PRODUCT_TYPE: Columns.BOX_TYPE}, inplace=True)
            input_box_type_df = input_df[[Columns.BOX_TYPE]]
            _format_and_validate_box_type(df=input_box_type_df)
            assert input_box_type_df.equals(output_df[[Columns.BOX_TYPE]])

            input_name_df = input_df[[Columns.NAME]]
            _format_and_validate_name(df=input_name_df)
            assert input_name_df.equals(output_df[[Columns.NAME]])

            input_phone_df = input_df[[Columns.PHONE]]
            _format_and_validate_phone(df=input_phone_df)
            assert input_phone_df.equals(output_df[[Columns.PHONE]])

    @pytest.mark.parametrize(
        "cell, expected_value",
        [
            ("A1", "DRIVER SUPPORT: 555-555-5555"),
            ("B1", None),
            ("C1", None),
            ("D1", "RECIPIENT SUPPORT: 555-555-5555 x5"),
            ("E1", None),
            ("F1", "PLEASE SHRED MANIFEST AFTER COMPLETING ROUTE."),
        ],
    )
    def test_header_row(
        self, cell: str, expected_value: str, basic_manifest_workbook: Workbook
    ) -> None:
        """Test that the header row is correct."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws[cell].value == expected_value

    def test_header_row_end(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the header row ends at F1."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            last_non_empty_col = max(
                (cell.column for cell in ws[1] if cell.value), default=None
            )
            assert last_non_empty_col == 6

    @pytest.mark.parametrize("cell", ["A1", "B1", "C1", "D1", "E1", "F1"])
    def test_header_row_color(self, cell: str, basic_manifest_workbook: Workbook) -> None:
        """Test the header row fill color."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws[cell].fill.start_color.rgb == f"{CellColors.HEADER}"

    def test_date_cell(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the date cell is correct."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws["A3"].value == f"Date: {MANIFEST_DATE}"

    def test_driver_cell(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the driver cell is correct."""
        drivers = [driver.upper() for driver in DRIVERS]
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            driver_name = sheet_name.replace(f"{MANIFEST_DATE} ", "")
            assert ws["A5"].value == f"Driver: {driver_name}"
            assert driver_name.upper() in drivers

    def test_agg_cells(
        self, basic_manifest_workbook: Workbook, mock_route_tables: Path
    ) -> None:
        """Test that the aggregated cells are correct."""
        for sheet_name in sorted(basic_manifest_workbook.sheetnames):
            driver = str(sheet_name).replace(f"{MANIFEST_DATE} ", "")
            input_df = pd.read_csv(mock_route_tables / f"{driver}.csv")
            ws = basic_manifest_workbook[sheet_name]

            input_df.rename(columns={Columns.PRODUCT_TYPE: Columns.BOX_TYPE}, inplace=True)
            agg_dict = _aggregate_route_data(df=input_df)

            neighborhoods = ", ".join(agg_dict["neighborhoods"])
            assert ws["A7"].value == f"Neighborhoods: {neighborhoods.upper()}"
            assert ws["E3"].value == BoxType.BASIC
            assert ws["F3"].value == agg_dict["box_counts"][BoxType.BASIC]
            assert ws["E4"].value == BoxType.GF
            assert ws["F4"].value == agg_dict["box_counts"][BoxType.GF]
            assert ws["E5"].value == BoxType.LA
            assert ws["F5"].value == agg_dict["box_counts"][BoxType.LA]
            assert ws["E6"].value == BoxType.VEGAN
            assert ws["F6"].value == agg_dict["box_counts"][BoxType.VEGAN]
            assert ws["E7"].value == "TOTAL BOX COUNT="
            assert ws["F7"].value == agg_dict["total_box_count"]
            assert ws["E8"].value == "PROTEIN COUNT="
            assert ws["F8"].value == agg_dict["protein_box_count"]

    def test_box_type_cell_colors(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the box type cells conditionally formatted with fill color."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            for cell in ws["F"]:
                if cell.row > 9:
                    assert cell.fill.start_color.rgb == f"{BOX_TYPE_COLOR_MAP[cell.value]}"
            for cell in ws["E"]:
                if cell.row > 2 and cell.row < 7:
                    assert cell.fill.start_color.rgb == f"{BOX_TYPE_COLOR_MAP[cell.value]}"

    def test_notes_column_width(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the notes column width is correct."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws["E9"].value == Columns.NOTES
            assert ws.column_dimensions["E"].width == NOTES_COLUMN_WIDTH

    @pytest.mark.parametrize(
        "cell",
        [
            # Header row.
            "A1",
            "B1",
            "C1",
            "D1",
            "E1",
            "F1",
            # Aggregated data.
            "A3",
            "A5",
            "A7",
            "E3",
            "E4",
            "E5",
            "E6",
            "E7",
            "E8",
            "F3",
            "F4",
            "F5",
            "F6",
            "F7",
            "F8",
            # Data header.
            "A9",
            "B9",
            "C9",
            "D9",
            "E9",
            "F9",
        ],
    )
    def test_bold_cells(self, cell: str, basic_manifest_workbook: Workbook) -> None:
        """Test that the cells are bold."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            assert ws[cell].font.bold

    def test_cell_right_alignment(self, basic_manifest_workbook: Workbook) -> None:
        """Test right-aligned cells."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            right_aligned_cells = [ws["D1"], ws["F1"]] + [
                cell for row in ws["E3:F8"] for cell in row
            ]
            for cell in right_aligned_cells:
                assert cell.alignment.horizontal == "right"

    def test_cell_left_alignment(self, basic_manifest_workbook: Workbook) -> None:
        """Test left-aligned cells."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            left_aligned_cells = [cell for row in ws["A1:A8"] for cell in row] + [
                cell for row in ws["A9:F9"] for cell in row
            ]
            for cell in left_aligned_cells:
                assert cell.alignment.horizontal == "left"


@pytest.mark.parametrize(
    "route_df, expected_agg_dict, error_context",
    [
        (
            pd.DataFrame(
                {
                    Columns.BOX_TYPE: ["BASIC", "GF", "LA", "BASIC", "GF", "LA", "Vegan"],
                    Columns.ORDER_COUNT: [1, 1, 1, 2, 1, 1, 2],
                    Columns.NEIGHBORHOOD: [
                        "YORK",
                        "YORK",
                        "YORK",
                        "PUGET",
                        "YORK",
                        "YORK",
                        "PUGET",
                    ],
                }
            ),
            {
                "box_counts": {"BASIC": 3, "GF": 2, "LA": 2, "VEGAN": 2},
                "total_box_count": 9,
                "protein_box_count": 7,
                "neighborhoods": ["YORK", "PUGET"],
            },
            nullcontext(),
        ),
        (
            pd.DataFrame(
                {
                    Columns.BOX_TYPE: ["BASIC", "GF", "LA", "BASIC", "GF", "LA"],
                    Columns.ORDER_COUNT: [1, 1, 1, 2, 1, 1],
                    Columns.NEIGHBORHOOD: ["YORK", "YORK", "YORK", "PUGET", "YORK", "YORK"],
                }
            ),
            {
                "box_counts": {"BASIC": 3, "GF": 2, "LA": 2, "VEGAN": 0},
                "total_box_count": 7,
                "protein_box_count": 7,
                "neighborhoods": ["YORK", "PUGET"],
            },
            nullcontext(),
        ),
        (
            pd.DataFrame(
                {
                    Columns.BOX_TYPE: [
                        "BASIC",
                        "GF",
                        "LA",
                        "BASIC",
                        "GF",
                        "LA",
                        "Vegan",
                        "bad box type",
                    ],
                    Columns.ORDER_COUNT: [1, 1, 1, 2, 1, 1, 2, 1],
                    Columns.NEIGHBORHOOD: [
                        "YORK",
                        "YORK",
                        "YORK",
                        "PUGET",
                        "YORK",
                        "YORK",
                        "PUGET",
                        "PUGET",
                    ],
                }
            ),
            {},
            pytest.raises(
                ValueError,
                match=re.escape("Invalid box type in route data: {'BAD BOX TYPE'}"),
            ),
        ),
    ],
)
def test_aggregate_route_data(
    route_df: pd.DataFrame, expected_agg_dict: dict, error_context: AbstractContextManager
) -> None:
    """Test that a route's data is aggregated correctly."""
    with error_context:
        agg_dict = _aggregate_route_data(df=route_df)
        assert agg_dict == expected_agg_dict


def _get_driver_sheets(output_paths: list[Path]) -> list[pd.DataFrame]:
    driver_sheets = []
    for output_path in output_paths:
        workbook = pd.ExcelFile(output_path)
        driver_sheets = driver_sheets + [
            pd.read_excel(workbook, sheet_name=sheet) for sheet in workbook.sheet_names
        ]

    return driver_sheets
