"""Test the sheet writing functions."""

import logging
from contextlib import AbstractContextManager, nullcontext
from unittest.mock import patch

import pytest
from openpyxl import Workbook

from bfb_delivery.lib.formatting.sheet_shaping import _add_header_row


@pytest.mark.parametrize(
    "mock_context, expected_warning, test_cell, expected_cell_value",
    [
        (nullcontext(), "", "A1", "DRIVER SUPPORT: 555-555-5555"),
        (nullcontext(), "", "D1", "RECIPIENT SUPPORT: 555-555-5555 x5"),
        # Mock no file.
        (
            patch("os.path.exists", return_value=False),
            "Config file not found: ",
            "A1",
            (
                "DRIVER SUPPORT: NO PHONE NUMBER. See warning in logs for instructions on "
                "setting up your config file."
            ),
        ),
        (
            patch("os.path.exists", return_value=False),
            "Config file not found: ",
            "D1",
            (
                "RECIPIENT SUPPORT: NO PHONE NUMBER. See warning in logs for instructions on "
                "setting up your config file."
            ),
        ),
        # Mock no number.
        (
            patch("configparser.ConfigParser.read", return_value=[]),
            " not found in config file: ",
            "A1",
            (
                "DRIVER SUPPORT: NO PHONE NUMBER. See warning in logs for instructions on "
                "setting up your config file."
            ),
        ),
        (
            patch("configparser.ConfigParser.read", return_value=[]),
            " not found in config file: ",
            "D1",
            (
                "RECIPIENT SUPPORT: NO PHONE NUMBER. See warning in logs for instructions on "
                "setting up your config file."
            ),
        ),
    ],
)
def test_add_header_row_phone_numbers(
    mock_context: AbstractContextManager,
    expected_warning: str,
    test_cell: str,
    expected_cell_value: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Correct phone numbers added to header row, and bad config warnings."""
    wb = Workbook()
    ws = wb.active
    with mock_context:
        if expected_warning:
            with caplog.at_level(logging.WARNING):
                if ws is not None:
                    _add_header_row(ws=ws)
                    assert expected_warning in caplog.text
        else:
            if ws is not None:
                _add_header_row(ws=ws)

    if ws is not None:
        assert ws[test_cell].value == expected_cell_value
    else:
        raise ValueError("Worksheet is None.")
