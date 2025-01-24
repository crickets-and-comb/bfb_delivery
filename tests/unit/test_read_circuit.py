"""Tests for read_circuit module."""

import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Final
from unittest.mock import patch

import pytest

# from click.testing import CliRunner
from typeguard import typechecked

from bfb_delivery import create_manifests_from_circuit

# from bfb_delivery.cli import (
#     create_manifests_from_circuit as create_manifests_from_circuit_cli,
# )
from bfb_delivery.lib.constants import ALL_HHS_DRIVER, FILE_DATE_FORMAT

# from bfb_delivery.lib.constants import ALL_HHS_DRIVER, FILE_DATE_FORMAT

# Don't really need this for tests right now, but it's the date of the test data.
TEST_START_DATE: Final[str] = "2025-01-17"


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


@pytest.mark.usefixtures(
    "mock_get_plan_responses", "mock_phonenumbers_parse", "mock_phonenumbers_is_valid_number"
)
class TestCreateManifestsFromCircuit:
    """Test create_manifests_from_circuit function."""

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
    @pytest.mark.parametrize("verbose", [True, False])
    def test_set_output_dir(
        self,
        all_HHs: bool,
        mock_stops_responses_fixture: str,
        verbose: bool,
        mock_driver_sheet_names_fixture: str,
        tmp_path: Path,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test that the output directory can be set."""
        output_dir = str(tmp_path / "dummy_output_dir")
        circuit_output_dir = f"{output_dir}/dummy_circuit_output"
        stops_response_data = request.getfixturevalue(mock_stops_responses_fixture)
        driver_sheet_names = request.getfixturevalue(mock_driver_sheet_names_fixture)

        with patch(
            "bfb_delivery.lib.dispatch.read_circuit._get_raw_stops_list",
            return_value=stops_response_data,
        ):
            output_path = create_manifests_from_circuit(
                start_date=TEST_START_DATE,
                output_dir=output_dir,
                circuit_output_dir=circuit_output_dir,
                all_HHs=all_HHs,
                verbose=verbose,
            )

        expected_output_filename = (
            f"final_manifests_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
        )
        expected_output_path = Path(output_dir) / expected_output_filename
        assert str(output_path) == str(expected_output_path)
        assert expected_output_path.exists()

        expected_circuit_output_dir = Path(circuit_output_dir)
        assert expected_circuit_output_dir.exists()

        circuit_files = [path.name for path in list(expected_circuit_output_dir.glob("*"))]
        expected_files = [f"{sheet_name}.csv" for sheet_name in driver_sheet_names]
        assert sorted(circuit_files) == sorted(expected_files)

    # @pytest.mark.parametrize("output_filename", ["", "dummy_output_filename.xlsx"])
    # # TODO: Change circuit_output_dir default to subdir in output_dir.
    # # @pytest.mark.parametrize("circuit_output_dir", ["dummy_circuit_output", ""])
    # # @pytest.mark.parametrize("all_hhs", [True, False])
    # @pytest.mark.parametrize("verbose", [True, False])
    # @typechecked
    # def test_cli(
    #     self,
    #     output_filename: str,
    #     # all_hhs: bool,
    #     verbose: bool,
    #     mock_driver_sheet_names_all_hhs_false: list[str],
    #     cli_runner: CliRunner,
    #     tmp_path: Path,
    # ) -> None:
    #     """Test CLI works."""
    #     output_dir = str(tmp_path / "dummy_output_dir")
    #     circuit_output_dir = f"{output_dir}/dummy_circuit_output"
    #     arg_list = [
    #         "--start_date",
    #         TEST_START_DATE,
    #         "--output_dir",
    #         output_dir,
    #         "--output_filename",
    #         output_filename,
    #         "--circuit_output_dir",
    #         circuit_output_dir,
    #     ]
    #     # if all_hhs:
    #     #     arg_list.append("--all_hhs")
    #     if verbose:
    #         arg_list.append("--verbose")

    #     result = cli_runner.invoke(create_manifests_from_circuit_cli.main, arg_list)
    #     assert result.exit_code == 0

    #     expected_output_filename = (
    #         f"final_manifests_{datetime.now().strftime(FILE_DATE_FORMAT)}.xlsx"
    #         if output_filename == ""
    #         else output_filename
    #     )
    #     expected_output_path = Path(output_dir)
    #     assert (expected_output_path / expected_output_filename).exists()

    #     expected_circuit_output_dir = Path(circuit_output_dir)
    #     assert expected_circuit_output_dir.exists()
    #     circuit_files = expected_circuit_output_dir.glob("*")
    #     assert sorted(circuit_files) == sorted(
    #         f"{sheet_name}.csv" for sheet_name in mock_driver_sheet_names_all_hhs_false
    #     )
