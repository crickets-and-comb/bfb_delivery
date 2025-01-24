"""Tests for read_circuit module."""

import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Final
from unittest.mock import patch

import pandas as pd
import pytest
from click.testing import CliRunner
from openpyxl import Workbook, load_workbook
from typeguard import typechecked

from bfb_delivery import create_manifests_from_circuit
from bfb_delivery.cli import (
    create_manifests_from_circuit as create_manifests_from_circuit_cli,
)
from bfb_delivery.lib.constants import (
    ALL_HHS_DRIVER,
    FILE_DATE_FORMAT,
    FORMATTED_ROUTES_COLUMNS,
    CellColors,
    Columns,
)
from bfb_delivery.lib.formatting.data_cleaning import (
    _format_and_validate_box_type,
    _format_and_validate_name,
    _format_and_validate_phone,
)

TEST_START_DATE: Final[str] = "2025-01-17"
MANIFEST_DATE: Final[str] = "1.17"


@pytest.fixture()
@typechecked
def mock_plan_responses() -> (
    list[dict[str, str | list[dict[str, str | dict[str, int]] | None]]]
):
    """Return a list of plan responses, as from _get_plan_responses."""
    with open("tests/unit/fixtures/plan_responses.json") as f:
        return json.load(f)


@pytest.fixture()
@typechecked
def mock_stops_responses_all_hhs_false() -> (
    list[
        list[
            dict[
                str,
                str
                | list[
                    dict[
                        str,
                        str
                        | int
                        | dict[
                            str, str | int | dict[str, str | int | list[str] | None] | None
                        ]
                        | None,
                    ]
                ]
                | None,
            ]
        ]
    ]
):
    """Return a list of stops responses, as from _get_raw_stops_list."""
    with open("tests/unit/fixtures/stops_responses.json") as f:
        return json.load(f)


@pytest.fixture()
@typechecked
def mock_stops_responses_all_hhs_true() -> (
    list[
        list[
            dict[
                str,
                str
                | list[
                    dict[
                        str,
                        str
                        | int
                        | dict[
                            str, str | int | dict[str, str | int | list[str] | None] | None
                        ]
                        | None,
                    ]
                ]
                | None,
            ]
        ]
    ]
):
    """Return a list of stops responses, as from _get_raw_stops_list."""
    with open("tests/unit/fixtures/stops_responses_all_hhs.json") as f:
        return json.load(f)


@pytest.fixture()
@typechecked
def mock_get_plan_responses(
    mock_plan_responses: list[dict[str, str | list[dict[str, str | dict[str, int]] | None]]]
) -> Iterator[None]:
    """Mock _get_plan_responses."""
    with patch(
        "bfb_delivery.lib.dispatch.read_circuit._get_plan_responses",
        return_value=mock_plan_responses,
    ):
        yield


@pytest.fixture()
@typechecked
def mock_driver_sheet_names_all_hhs_false(
    mock_plan_responses: list[
        dict[str, str | list[dict[str, str | list[str | dict[str, str]] | dict[str, int]]]]
    ]
) -> list[str]:
    """Return a list of driver sheet names."""
    driver_sheet_names = []
    for page_dict in mock_plan_responses:
        for plan_dict in page_dict["plans"]:
            if (
                isinstance(plan_dict, dict)  # To satisisfy pytype.
                and isinstance(plan_dict["title"], str)  # To satisisfy pytype.
                and ALL_HHS_DRIVER not in plan_dict["title"]
            ):
                driver_sheet_names.append(plan_dict["title"])

    return driver_sheet_names


@pytest.fixture()
@typechecked
def mock_driver_names_all_hhs_false(
    mock_driver_sheet_names_all_hhs_false: list[str],
) -> list[str]:
    """Return a list of driver names."""
    return [
        " ".join(sheet_name.split(" ")[1:])
        for sheet_name in mock_driver_sheet_names_all_hhs_false
    ]


@pytest.fixture()
@typechecked
def mock_driver_sheet_names_all_hhs_true(
    mock_plan_responses: list[
        dict[str, str | list[dict[str, str | list[str | dict[str, str]] | dict[str, int]]]]
    ]
) -> list[str]:
    """Return a list of driver sheet names."""
    driver_sheet_names = []
    for page_dict in mock_plan_responses:
        for plan_dict in page_dict["plans"]:
            if (
                isinstance(plan_dict, dict)  # To satisisfy pytype.
                and isinstance(plan_dict["title"], str)  # To satisisfy pytype.
                and ALL_HHS_DRIVER in plan_dict["title"]
            ):
                driver_sheet_names.append(plan_dict["title"])

    return driver_sheet_names


@pytest.fixture()
def mock_phonenumbers_parse() -> Iterator[None]:
    """Mock phonenumbers.parse."""
    with patch("phonenumbers.parse", side_effect=lambda x, *_: x):
        yield


@pytest.fixture()
def mock_phonenumbers_is_valid_number() -> Iterator[None]:
    """Mock phonenumbers.is_valid_number."""
    with patch("phonenumbers.is_valid_number", return_value=True):
        yield


@pytest.fixture()
def mock_os_getcwd(tmp_path: Path) -> Iterator[str]:
    """Mock os.getcwd within the read_circuit module."""
    return_value = str(tmp_path)
    with patch("bfb_delivery.lib.dispatch.read_circuit._getcwd", return_value=return_value):
        yield return_value


@pytest.mark.usefixtures(
    "mock_get_plan_responses",
    "mock_phonenumbers_parse",
    "mock_phonenumbers_is_valid_number",
    "mock_os_getcwd",
)
class TestCreateManifestsFromCircuit:
    """Test create_manifests_from_circuit function."""

    @pytest.fixture()
    def basic_outputs(
        self, mock_stops_responses_all_hhs_false: list, tmp_path: Path
    ) -> tuple[Path, Path]:
        """Create a basic manifest scoped to class for reuse."""
        with patch(
            "bfb_delivery.lib.dispatch.read_circuit._get_raw_stops_list",
            return_value=mock_stops_responses_all_hhs_false,
        ):
            manifest_path, circuit_sheets_dir = create_manifests_from_circuit(
                start_date=TEST_START_DATE, output_dir=str(tmp_path)
            )
        return manifest_path, circuit_sheets_dir

    @pytest.fixture()
    def basic_manifest_workbook(self, basic_outputs: tuple[Path, Path]) -> Workbook:
        """Create a basic manifest workbook scoped to class for reuse."""
        workbook = load_workbook(basic_outputs[0])
        return workbook

    @pytest.fixture()
    def basic_manifest_ExcelFile(
        self, basic_outputs: tuple[Path, Path]
    ) -> Iterator[pd.ExcelFile]:
        """Create a basic manifest workbook scoped to class for reuse."""
        with pd.ExcelFile(basic_outputs[0]) as xls:
            yield xls

    @pytest.mark.parametrize("circuit_output_dir", ["dummy_circuit_output", ""])
    @pytest.mark.parametrize(
        "all_HHs, mock_stops_responses_fixture",
        [
            (True, "mock_stops_responses_all_hhs_true"),
            (False, "mock_stops_responses_all_hhs_false"),
        ],
    )
    @pytest.mark.parametrize("verbose", [True, False])
    @pytest.mark.parametrize("test_cli", [False, True])
    def test_set_output_dir(
        self,
        circuit_output_dir: str,
        all_HHs: bool,
        mock_stops_responses_fixture: str,
        verbose: bool,
        test_cli: bool,
        mock_os_getcwd: str,
        tmp_path: Path,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test that the output directory can be set."""
        stops_response_data = request.getfixturevalue(mock_stops_responses_fixture)

        output_dir = str(tmp_path / "dummy_output_dir")
        expected_output_filename = (
            f"final_manifests_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
        )
        expected_output_path = Path(output_dir) / expected_output_filename

        circuit_output_dir = (
            str(tmp_path / circuit_output_dir) if circuit_output_dir else circuit_output_dir
        )
        circuit_sub_dir = "routes_" + TEST_START_DATE
        expected_circuit_output_dir = (
            Path(circuit_output_dir) / circuit_sub_dir
            if circuit_output_dir
            else Path(mock_os_getcwd) / circuit_sub_dir
        )

        Path(expected_circuit_output_dir).mkdir(parents=True, exist_ok=True)
        with open(f"{expected_circuit_output_dir}/dummy_file.txt", "w") as f:
            f.write("Dummy file. The function should remove this file.")

        with patch(
            "bfb_delivery.lib.dispatch.read_circuit._get_raw_stops_list",
            return_value=stops_response_data,
        ):
            if test_cli:
                cli_runner = CliRunner()
                arg_list = [
                    "--start_date",
                    TEST_START_DATE,
                    "--output_dir",
                    output_dir,
                    "--circuit_output_dir",
                    circuit_output_dir,
                ]
                if all_HHs:
                    arg_list.append("--all_hhs")
                if verbose:
                    arg_list.append("--verbose")
                result = cli_runner.invoke(create_manifests_from_circuit_cli.main, arg_list)
                assert result.exit_code == 0
                output_path, new_circuit_output_dir = (
                    result.stdout_bytes.decode("utf-8").strip().split("\n")
                )
            else:
                output_path, new_circuit_output_dir = create_manifests_from_circuit(
                    start_date=TEST_START_DATE,
                    output_dir=output_dir,
                    circuit_output_dir=circuit_output_dir,
                    all_HHs=all_HHs,
                    verbose=verbose,
                )

        assert str(new_circuit_output_dir) == str(expected_circuit_output_dir)
        assert str(output_path) == str(expected_output_path)
        assert expected_output_path.exists()

    @pytest.mark.parametrize(
        "all_HHs, mock_stops_responses_fixture, mock_driver_sheet_names_fixture",
        [
            (
                True,
                "mock_stops_responses_all_hhs_true",
                "mock_driver_sheet_names_all_hhs_true",
            ),
            (
                False,
                "mock_stops_responses_all_hhs_false",
                "mock_driver_sheet_names_all_hhs_false",
            ),
        ],
    )
    def test_all_drivers_have_a_sheet(
        self,
        tmp_path: Path,
        all_HHs: bool,
        mock_stops_responses_fixture: str,
        mock_driver_sheet_names_fixture: str,
        mock_os_getcwd: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test that all drivers have a sheet in the formatted workbook. And date works."""
        stops_response_data = request.getfixturevalue(mock_stops_responses_fixture)
        driver_sheet_names = request.getfixturevalue(mock_driver_sheet_names_fixture)
        with patch(
            "bfb_delivery.lib.dispatch.read_circuit._get_raw_stops_list",
            return_value=stops_response_data,
        ):
            output_path, circuit_output_dir = create_manifests_from_circuit(
                start_date=TEST_START_DATE, output_dir=str(tmp_path), all_HHs=all_HHs
            )
        workbook = pd.ExcelFile(output_path)
        assert sorted(list(workbook.sheet_names)) == sorted(driver_sheet_names)
        assert sorted([path.name for path in list(circuit_output_dir.glob("*"))]) == sorted(
            [f"{sheet_name}.csv" for sheet_name in driver_sheet_names]
        )

    def test_date_field_matches_sheet_date(self, basic_manifest_workbook: Workbook) -> None:
        """Test that the date field matches the sheet date."""
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            field_date = ws["A3"].value.split(" ")[1]
            sheet_name_date = sheet_name.split(" ")[0]
            assert field_date == sheet_name_date

    def test_df_is_same(
        self, basic_outputs: tuple[Path, Path], basic_manifest_ExcelFile: pd.ExcelFile
    ) -> None:
        """All the input data is in the formatted workbook."""
        for sheet_name in sorted(basic_manifest_ExcelFile.sheet_names):
            input_df = pd.read_csv(basic_outputs[1] / f"{sheet_name}.csv")
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

    def test_driver_cell(
        self, mock_driver_names_all_hhs_false: list[str], basic_manifest_workbook: Workbook
    ) -> None:
        """Test that the driver cell is correct."""
        drivers = [driver.upper() for driver in mock_driver_names_all_hhs_false]
        for sheet_name in basic_manifest_workbook.sheetnames:
            ws = basic_manifest_workbook[sheet_name]
            driver_name = sheet_name.replace(f"{MANIFEST_DATE} ", "")
            assert ws["A5"].value == f"Driver: {driver_name}"
            assert driver_name.upper() in drivers
