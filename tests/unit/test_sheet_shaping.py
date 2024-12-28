"""Unit tests for sheet_shaping.py."""

from datetime import datetime
from pathlib import Path
from typing import Final

import pandas as pd
import pytest
from click.testing import CliRunner

from bfb_delivery import combine_route_tables, format_combined_routes, split_chunked_route
from bfb_delivery.cli import combine_route_tables as combine_route_tables_cli
from bfb_delivery.cli import format_combined_routes as format_combined_routes_cli
from bfb_delivery.cli import split_chunked_route as split_chunked_route_cli
from bfb_delivery.lib.constants import COMBINED_ROUTES_COLUMNS, SPLIT_ROUTE_COLUMNS, Columns
from bfb_delivery.lib.formatting.data_cleaning import (
    _format_and_validate_phone_column,
    _format_box_type_column,
    _format_name_column,
    _format_neighborhood_column,
)

N_BOOKS_MATRIX: Final[list[int]] = [1, 3, 4]
DRIVERS: Final[list[str]] = [f"Driver {i}" for i in range(1, 10)]
BOX_TYPES: Final[list[str]] = ["Basic", "GF", "Vegan", "LA"]
NEIGHBORHOODS: Final[list[str]] = ["York", "Puget", "Samish", "Sehome", "South Hill"]


@pytest.fixture(scope="module")
def module_tmp_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Get a temporary directory for the class."""
    return tmp_path_factory.mktemp("tmp")


@pytest.fixture(scope="module")
def mock_chunked_sheet_raw(module_tmp_dir: Path) -> Path:
    """Save mock chunked route sheet and get path."""
    fp: Path = module_tmp_dir / "mock_chunked_sheet_raw.xlsx"
    # TODO: Use specific sheet name?
    raw_chunked_sheet = pd.DataFrame(
        columns=SPLIT_ROUTE_COLUMNS + [Columns.DRIVER, Columns.BOX_COUNT, Columns.STOP_NO],
        # TODO: Validate box count.
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
    )
    raw_chunked_sheet.to_excel(fp, index=False)

    return fp


class TestCombineRouteTables:
    """combine_route_tables combines driver route CSVs into a single workbook."""

    @pytest.fixture(scope="class")
    def mock_route_tables(
        self, module_tmp_dir: Path, mock_chunked_sheet_raw: Path
    ) -> list[Path]:
        """Mock the driver route tables returned by Circuit."""
        output_paths = []
        output_cols = [Columns.STOP_NO] + SPLIT_ROUTE_COLUMNS
        chunked_df = pd.read_excel(mock_chunked_sheet_raw)
        for driver in chunked_df[Columns.DRIVER].unique():
            output_path = module_tmp_dir / f"{driver}.csv"
            output_paths.append(output_path)
            driver_df = chunked_df[chunked_df[Columns.DRIVER] == driver]
            driver_df[Columns.STOP_NO] = [i + 1 for i in range(len(driver_df))]
            driver_df[output_cols].to_csv(module_tmp_dir / output_path, index=False)

        return output_paths

    @pytest.mark.parametrize("output_dir_type", [Path, str])
    @pytest.mark.parametrize("output_dir", ["", "dummy_output"])
    def test_set_output_dir(
        self,
        output_dir_type: type[Path | str],
        output_dir: Path | str,
        module_tmp_dir: Path,
        mock_route_tables: list[Path],
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = output_dir_type(module_tmp_dir / output_dir)
        output_path = combine_route_tables(
            input_paths=mock_route_tables, output_dir=output_dir
        )
        assert str(output_path.parent) == str(output_dir)

    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.csv"])
    def test_set_output_filename(
        self, output_filename: str, mock_route_tables: list[Path]
    ) -> None:
        """Test that the output filename can be set."""
        output_path = combine_route_tables(
            input_paths=mock_route_tables, output_filename=output_filename
        )
        expected_filename = (
            # TODO: Make date format constant.
            f"combined_routes_{datetime.now().strftime('%Y%m%d')}.xlsx"
            if output_filename == ""
            else output_filename
        )
        assert output_path.name == expected_filename

    # TODO: Test output columns.
    def test_output_columns(self, mock_route_tables: list[Path]) -> None:
        """Test that the output columns match the COMBINED_ROUTES_COLUMNS constant."""
        output_path = combine_route_tables(input_paths=mock_route_tables)
        workbook = pd.ExcelFile(output_path)
        for sheet_name in workbook.sheet_names:
            driver_sheet = pd.read_excel(workbook, sheet_name=sheet_name)
            assert driver_sheet.columns.to_list() == COMBINED_ROUTES_COLUMNS

    def test_unique_recipients(self, mock_route_tables: list[Path]) -> None:
        """Test that the recipients don't overlap between the driver route tables.

        By name, address, and phone.
        """
        output_path = combine_route_tables(input_paths=mock_route_tables)
        driver_sheets = _get_driver_sheets(output_paths=[output_path])
        combined_output_data = pd.concat(driver_sheets, ignore_index=True)
        assert (
            combined_output_data[[Columns.NAME, Columns.ADDRESS, Columns.PHONE]]
            .duplicated()
            .sum()
            == 0  # noqa: W503
        )

    def test_complete_contents(self, mock_route_tables: list[Path]) -> None:
        """Test that the input data is all covered in the combined workbook."""
        output_path = combine_route_tables(input_paths=mock_route_tables)

        full_input_data = pd.concat(
            [pd.read_csv(path)[COMBINED_ROUTES_COLUMNS] for path in mock_route_tables],
            ignore_index=True,
        )
        driver_sheets = _get_driver_sheets(output_paths=[output_path])
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
        cli_runner: CliRunner,
        mock_route_tables: list[Path],
        module_tmp_dir: Path,
    ) -> None:
        """Test CLI works."""
        output_dir = str(module_tmp_dir / output_dir) if output_dir else output_dir
        arg_list = ["--output_dir", output_dir, "--output_filename", output_filename]
        for path in mock_route_tables:
            arg_list.append("--input_paths")
            arg_list.append(str(path))

        result = cli_runner.invoke(combine_route_tables_cli.main, arg_list)
        assert result.exit_code == 0

        expected_output_filename = (
            f"combined_routes_{datetime.now().strftime('%Y%m%d')}.xlsx"
            if output_filename == ""
            else output_filename
        )
        expected_output_dir = Path(output_dir) if output_dir else mock_route_tables[0].parent
        assert (expected_output_dir / expected_output_filename).exists()


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
                else f"split_workbook_{datetime.now().strftime('%Y%m%d')}_{i + 1}.xlsx"
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
        _format_and_validate_phone_column(df=phone_df)
        assert phone_df[Columns.PHONE].equals(split_data[Columns.PHONE])

        name_df = full_data[[Columns.NAME]].copy()
        _format_name_column(df=name_df)
        assert name_df[Columns.NAME].equals(split_data[Columns.NAME])

        box_type_df = full_data[[Columns.BOX_TYPE]].copy()
        _format_box_type_column(df=box_type_df)
        assert box_type_df[Columns.BOX_TYPE].equals(split_data[Columns.BOX_TYPE])

        neighborhood_df = full_data[[Columns.NEIGHBORHOOD]].copy()
        _format_neighborhood_column(df=neighborhood_df)
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
        cli_runner: CliRunner,
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

        result = cli_runner.invoke(split_chunked_route_cli.main, arg_list)
        assert result.exit_code == 0

        for i in range(n_books):
            expected_filename = (
                f"{output_filename.split('.')[0]}_{i + 1}.xlsx"
                if output_filename
                else f"split_workbook_{datetime.now().strftime('%Y%m%d')}_{i + 1}.xlsx"
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


class TestFormatCombinedRoutes:
    """format_combined_routes formats the combined routes table."""

    @pytest.fixture(scope="class")
    def mock_combined_routes(
        self, module_tmp_dir: Path, mock_chunked_sheet_raw: Path
    ) -> Path:
        """Mock the combined routes table."""
        output_path = module_tmp_dir / "combined_routes.xlsx"
        with pd.ExcelWriter(output_path) as writer:
            for driver in DRIVERS:
                df = pd.DataFrame(columns=COMBINED_ROUTES_COLUMNS)
                stops = [stop_no + 1 for stop_no in range(9)]
                df[Columns.STOP_NO] = stops
                df[Columns.NAME] = [f"{driver} Recipient {stop_no}" for stop_no in stops]
                df[Columns.ADDRESS] = [
                    f"{driver} stop {stop_no} address" for stop_no in stops
                ]
                # TODO: Use phonenumbers.example_number for testing.
                df[Columns.PHONE] = ["13607345215"] * len(stops)
                df[Columns.NOTES] = [f"{driver} stop {stop_no} notes" for stop_no in stops]
                df[Columns.ORDER_COUNT] = [1] * len(stops)
                df[Columns.BOX_TYPE] = [
                    BOX_TYPES[i % len(BOX_TYPES)] for i in range(len(stops))
                ]
                df[Columns.NEIGHBORHOOD] = [
                    NEIGHBORHOODS[i % len(NEIGHBORHOODS)] for i in range(len(stops))
                ]

                assert df.isna().sum().sum() == 0
                assert set(df.columns.to_list()) == set(COMBINED_ROUTES_COLUMNS)

                df.to_excel(writer, sheet_name=driver, index=False)

        return output_path

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
            f"formatted_routes_{datetime.now().strftime('%Y%m%d')}.xlsx"
            if output_filename == ""
            else output_filename
        )
        assert output_path.name == expected_output_filename

    def test_all_drivers_have_a_sheet(self, mock_combined_routes: Path) -> None:
        """Test that all drivers have a sheet in the formatted workbook."""
        output_path = format_combined_routes(input_path=mock_combined_routes)
        workbook = pd.ExcelFile(output_path)
        assert set(workbook.sheet_names) == set(DRIVERS)

    @pytest.mark.parametrize("output_dir", ["dummy_output", ""])
    @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.xlsx"])
    def test_cli(
        self,
        output_dir: str,
        output_filename: str,
        cli_runner: CliRunner,
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

        result = cli_runner.invoke(format_combined_routes_cli.main, arg_list)
        assert result.exit_code == 0

        expected_output_filename = (
            f"formatted_routes_{datetime.now().strftime('%Y%m%d')}.xlsx"
            if output_filename == ""
            else output_filename
        )
        expected_output_dir = Path(output_dir) if output_dir else mock_combined_routes.parent
        assert (expected_output_dir / expected_output_filename).exists()


def _get_driver_sheets(output_paths: list[Path]) -> list[pd.DataFrame]:
    driver_sheets = []
    for output_path in output_paths:
        workbook = pd.ExcelFile(output_path)
        driver_sheets = driver_sheets + [
            pd.read_excel(workbook, sheet_name=sheet) for sheet in workbook.sheet_names
        ]

    return driver_sheets
