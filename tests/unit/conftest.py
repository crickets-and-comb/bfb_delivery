"""Conftest for unit tests."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from click.testing import CliRunner
from typeguard import typechecked

from bfb_delivery.lib.constants import SPLIT_ROUTE_COLUMNS, Columns


@pytest.fixture()
@typechecked
def cli_runner() -> CliRunner:
    """Get a CliRunner."""
    return CliRunner()


@pytest.fixture(autouse=True)
@typechecked
def mock_sleep() -> Iterator[None]:
    """Mock `time.sleep` to avoid waiting in tests."""
    with patch("comb_utils.lib.api_callers.sleep"):
        yield


@pytest.fixture()
@typechecked
def mock_is_valid_number() -> Iterator:
    """Mock phonenumbers.is_valid_number as True."""
    with patch("phonenumbers.is_valid_number", return_value=True):
        yield


@pytest.fixture(scope="module")
@typechecked
def mock_chunked_sheet_raw(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Save mock chunked route sheet and get path."""
    tmp_output = tmp_path_factory.mktemp(
        "tmp_mock_chunked_sheet_raw_class_scoped", numbered=True
    )
    fp: Path = tmp_output / "mock_chunked_sheet_raw.xlsx"
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
                "Driver A",
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
                "Driver A",
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
                "Driver B",
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
                "Driver B",
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
                "Driver C",
                1,
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
                "Driver D #1",
                1,
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
                "Driver D #2",
                2,
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
                "Driver D #2",
                None,
                8,
            ),
            (
                "Recipient Nine",
                "2122 Cedar St",
                "555-555-2223",
                "Recipient9@email.com",
                "Notes for Recipient Nine.",
                "1",
                "Basic",
                "South Hill",
                "Driver E",
                2,
                9,
            ),
            (
                "Recipient Ten",
                "2122 Cedar St",
                "555-555-2223",
                "Recipient10@email.com",
                "Notes for Recipient Ten.",
                "1",
                "LA",
                "South Hill",
                "Driver E",
                None,
                10,
            ),
            (
                "Recipient Eleven",
                "2346 Ash St",
                "555-555-2345",
                "Recipient11@email.com",
                "Notes for Recipient Eleven.",
                "1",
                "Basic",
                "Eldridge",
                "Driver F",
                2,
                11,
            ),
            (
                "Recipient Twelve",
                "2122 Cedar St",
                "555-555-2223",
                "Recipient12@email.com",
                "Notes for Recipient Twelve.",
                "1",
                "Basic",
                "Eldridge",
                "Driver F",
                None,
                12,
            ),
        ],
    ).rename(columns={Columns.PRODUCT_TYPE: Columns.BOX_TYPE})
    raw_chunked_sheet.to_excel(fp, index=False)

    return fp
