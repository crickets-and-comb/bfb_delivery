"""Unit tests for the data_cleaning module."""

import re
from contextlib import AbstractContextManager, nullcontext

import pandas as pd
import pytest

from bfb_delivery.lib.constants import Columns
from bfb_delivery.lib.formatting.data_cleaning import (
    format_and_validate_data,
    format_column_names,
)


class TestFormatColumnNames:
    """Test the format_column_names function."""

    def test_format_column_names(self) -> None:
        """Test formatting column names."""
        # TODO: Int column names? Is that possible?
        columns = ["  Name  ", Columns.ADDRESS, "  Phone  "]
        expected = [Columns.NAME, Columns.ADDRESS, Columns.PHONE]
        assert format_column_names(columns) == expected


class TestFormatAndValidateData:
    """Test the format_and_validate_data function."""

    @pytest.mark.parametrize(
        "column_name, expected_values",
        [  # TODO: Pull this out into a class-scoped fixture df. Useful input to next tests.
            (Columns.STOP_NO, [1, 2, 3, 4]),
            (Columns.NAME, ["Alice", "Bob", "Charlie", "David"]),
            (Columns.ADDRESS, ["123 Main St", "456 Elm St", "789 Oak St", "1011 Pine St"]),
            (Columns.PHONE, ["555-1234", "555-5678", "555-9012", "555-3456"]),
            (Columns.EMAIL, ["me@me.com", "you@me.com", "we@me.com", "me@you.com"]),
            (Columns.NOTES, ["", "", "", ""]),
            (Columns.ORDER_COUNT, [1, 1, 1, 1]),
            (Columns.BOX_TYPE, ["Basic", "Basic", "Basic", "Basic"]),
            (Columns.NEIGHBORHOOD, ["York", "York", "York", "York"]),
        ],
    )
    def test_format_and_validate_data(self, column_name: str, expected_values: list) -> None:
        """Test formatting and validating data."""
        # TODO: Pull this out into a class-scoped fixture to avoid repeated calls.
        columns = [
            Columns.STOP_NO,
            Columns.NAME,
            Columns.ADDRESS,
            Columns.PHONE,
            Columns.EMAIL,
            Columns.NOTES,
            Columns.ORDER_COUNT,
            Columns.BOX_TYPE,
            Columns.NEIGHBORHOOD,
        ]
        df = pd.DataFrame(
            columns=columns,
            data=[
                # TODO: As formatting is implemented, add/update rows and comment
                # what formatting is under test.
                (1, "Alice", "123 Main St", "555-1234", "me@me.com", "", 1, "Basic", "York"),
                (
                    " 2 ",  # Test trimming whitespace.
                    "Bob ",  # Test trimming whitespace.
                    "456 Elm St",
                    "555-5678",
                    "you@me.com",
                    "",
                    1,
                    "Basic",
                    "York",
                ),
                (
                    3.0,  # Test cast float.
                    "Charlie",
                    "789 Oak St",
                    "555-9012",
                    "we@me.com",
                    "",
                    1,
                    "Basic",
                    "York",
                ),
                (
                    "4.0 ",  # Test cast str float.
                    "David",
                    "1011 Pine St",
                    "555-3456",
                    "me@you.com",
                    "",
                    1,
                    "Basic",
                    "York",
                ),
            ],
        )
        format_and_validate_data(df, columns)
        assert df[column_name].to_list() == expected_values

    @pytest.mark.parametrize(
        "columns, df, expected_error_context",
        [
            (
                [Columns.STOP_NO, Columns.NAME],
                pd.DataFrame(columns=[Columns.STOP_NO, Columns.NAME], data=[(1, "Alice")]),
                nullcontext(),
            ),
            # Test missing columns raise an error.
            (
                [Columns.STOP_NO, Columns.NAME, Columns.EMAIL, Columns.ADDRESS],
                pd.DataFrame(columns=[Columns.STOP_NO, Columns.NAME], data=[(1, "Alice")]),
                pytest.raises(
                    ValueError,
                    match=(
                        re.escape(
                            "Columns not found in DataFrame: "
                            f"{[Columns.ADDRESS, Columns.EMAIL]}."
                        )
                    ),
                ),
            ),
            # Test columns not in formatters raise an error.
            (
                [Columns.STOP_NO, Columns.NAME, "extra_column"],
                pd.DataFrame(
                    columns=[Columns.STOP_NO, Columns.NAME, "extra_column"],
                    data=[(1, "Alice", "extra value")],
                ),
                pytest.raises(
                    ValueError, match="No formatter found for column: extra_column."
                ),
            ),
        ],
    )
    def test_invalid_column_names(
        self,
        columns: list[str],
        df: pd.DataFrame,
        expected_error_context: AbstractContextManager,
    ) -> None:
        """Test missing columns raise an error."""
        with expected_error_context:
            format_and_validate_data(df, columns)
