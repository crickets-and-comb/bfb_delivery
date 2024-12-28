"""Unit tests for the data_cleaning module."""

import re
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext

import pandas as pd
import pytest

from bfb_delivery.lib.constants import MAX_ORDER_COUNT, Columns
from bfb_delivery.lib.formatting.data_cleaning import (
    _validate_stop_no_column,
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
        assert format_column_names(columns=columns) == expected


class TestFormatAndValidateData:
    """Test the format_and_validate_data function."""

    @pytest.mark.parametrize(
        "column_name, expected_values",
        [  # TODO: Pull this out into a class-scoped fixture df. Useful input to next tests.
            (Columns.STOP_NO, [1, 2, 3, 4, 5]),
            (Columns.NAME, ["Alice", "Bob", "Charlie", "David", "Eve"]),
            (
                Columns.ADDRESS,
                ["123 Main St", "456 Elm St", "789 Oak St", "1011 Pine St", "1213 Cedar St"],
            ),
            (Columns.PHONE, ["555-1234", "555-5678", "555-9012", "555-3456", "555-7890"]),
            (
                Columns.EMAIL,
                ["me@me.com", "you@me.com", "we@me.com", "me@you.com", "you@you.com"],
            ),
            (Columns.NOTES, ["", "Drop the box.", "", "", ""]),
            (Columns.ORDER_COUNT, [1, 1, 1, 1, MAX_ORDER_COUNT]),
            (Columns.BOX_TYPE, ["Basic", "Basic", "Basic", "Basic", "Basic"]),
            (Columns.NEIGHBORHOOD, ["York", "York", "York", "York", "York"]),
        ],
    )
    def test_format_data(self, column_name: str, expected_values: list) -> None:
        """Test formatting and validating data."""
        # TODO: Pull this out into a class-scoped fixture to avoid repeated calls.
        columns = [
            Columns.DRIVER,
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
                # what formatting is under test. Leave perfect starter row.
                (
                    "Driver",
                    1,
                    "Alice",
                    "123 Main St",
                    "555-1234",
                    "me@me.com",
                    "",
                    1,
                    "Basic",
                    "York",
                ),
                (
                    " Driver",  # Test stripping whitespace.
                    " 2 ",  # Test stripping whitespace.
                    "Bob ",  # Test stripping whitespace.
                    " 456 Elm St",  # Test stripping whitespace.
                    " 555-5678 ",  # Test stripping whitespace.
                    "you@me.com ",  # Test stripping whitespace.
                    " Drop the box.",  # Test stripping whitespace.
                    "1 ",  # Test stripping whitespace.
                    " Basic ",  # Test stripping whitespace.
                    " York",  # Test stripping whitespace.
                ),
                (
                    "Driver",
                    3.0,  # Test cast float.
                    "Charlie",
                    "789 Oak St",
                    "555-9012",
                    # TODO: File issue with email_validator.
                    # Examples claim spaces are removed from domains.
                    "we@mE.cOm",  # Test domain case formatting.
                    "",
                    1.0,  # Test cast float.
                    "Basic",
                    "York",
                ),
                (
                    "Driver",
                    "4.0 ",  # Test cast str float.
                    "David",
                    "1011 Pine St",
                    "555-3456",
                    "me@you.com",
                    "",
                    "1.0",  # Test cast str float.
                    "Basic",
                    "York",
                ),
                (
                    "Driver",
                    5,
                    "Eve",
                    "1213 Cedar St",
                    "555-7890",
                    "you@you.com",
                    "",
                    MAX_ORDER_COUNT,  # Test max order count.
                    "Basic",
                    "York",
                ),
            ],
        )
        format_and_validate_data(df=df, columns=columns)
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
            format_and_validate_data(df=df, columns=columns)

    @pytest.mark.parametrize(
        "df, expected_error_context, validating_function",
        [
            (
                pd.DataFrame({Columns.ORDER_COUNT: [None]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.ORDER_COUNT: [""]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.ORDER_COUNT: [-1]}),
                pytest.raises(
                    ValueError,
                    match=(
                        "Values less than or equal to zero found in "
                        f"{Columns.ORDER_COUNT} column: "
                    ),
                ),
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.ORDER_COUNT: [MAX_ORDER_COUNT + 1]}),
                pytest.raises(
                    ValueError, match=f"Order count exceeds maximum of {MAX_ORDER_COUNT}: "
                ),
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.STOP_NO: [None]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.STOP_NO: [""]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.STOP_NO: [-1]}),
                pytest.raises(
                    ValueError,
                    match=(
                        "Values less than or equal to zero found in "
                        f"{Columns.STOP_NO} column: "
                    ),
                ),
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.STOP_NO: [1, 1]}),
                pytest.raises(ValueError, match="Duplicate stop numbers found: "),
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.STOP_NO: [1, 2, 4]}),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Stop numbers are not contiguous starting at 1: [1, 2, 4]"
                    ),
                ),
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.STOP_NO: [2, 3, 4]}),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Stop numbers are not contiguous starting at 1: [2, 3, 4]"
                    ),
                ),
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.STOP_NO: [1, 3, 2]}),
                pytest.raises(
                    ValueError, match=re.escape("Stop numbers are not sorted: [1, 3, 2]")
                ),
                _validate_stop_no_column,
            ),
            (
                pd.DataFrame(
                    {Columns.EMAIL: ["us@them..com", "u@s@them.com", "us@them.com"]}
                ),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Invalid email addresses found: ['us@them..com', 'u@s@them.com']"
                    ),
                ),
                format_and_validate_data,
            ),
        ],
    )
    def test_validations(
        self,
        df: pd.DataFrame,
        expected_error_context: AbstractContextManager,
        validating_function: Callable,
    ) -> None:
        """Test validations."""
        with expected_error_context:
            format_and_validate_data(df=df, columns=df.columns.to_list())
