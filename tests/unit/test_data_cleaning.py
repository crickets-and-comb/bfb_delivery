"""Unit tests for the data_cleaning module."""

import re
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Final

import pandas as pd
import phonenumbers
import pytest

from bfb_delivery.lib.constants import MAX_ORDER_COUNT, Columns
from bfb_delivery.lib.formatting.data_cleaning import (
    _validate_stop_no_column,
    format_and_validate_data,
    format_column_names,
)

INVALID_NUMBERS: Final[list[str]] = [
    "+1" + str(phonenumbers.invalid_example_number(region_code="US").national_number),
    "+15555555555",
]


class TestFormatColumnNames:
    """Test the format_column_names function."""

    def test_format_column_names(self) -> None:
        """Test formatting column names."""
        # TODO: Int column names? Is that possible?
        columns = ["  Name  ", Columns.ADDRESS, "  Phone  "]
        expected = [Columns.NAME, Columns.ADDRESS, Columns.PHONE]
        assert format_column_names(columns=columns) == expected


# TODO: Test nulls, empty strings, and whitespace.
class TestFormatAndValidateData:
    """Test the format_and_validate_data function."""

    @pytest.mark.usefixtures("mock_is_valid_number")
    @pytest.mark.parametrize(
        "column_name, expected_values",
        [  # TODO: Pull this out into a class-scoped fixture df. Useful input to next tests.
            (
                Columns.DRIVER,
                [
                    "DRIVER",
                    "DRIVER",
                    "BOATY MCBOATFACE",
                    "TIM #2",
                    "DRIVER",
                    "DRIVER",
                    "DRIVER",
                    "DRIVER",
                    "DRIVER",
                ],
            ),
            (Columns.STOP_NO, [1, 2, 3, 4, 5, 6, 7, 8, 9]),
            (
                Columns.NAME,
                [
                    "ALICE",
                    "BOB",
                    "CHARLIE BUCKET",
                    "DAVID & JONATHAN",
                    "EVE",
                    "FRANK",
                    "GINA",
                    "HANK",
                    "IVY",
                ],
            ),
            (
                Columns.ADDRESS,
                [
                    "123 Main St",
                    "456 Elm St",
                    "789 Oak St",
                    "1011 Pine St",
                    "1213 Cedar St",
                    "1315 Birch St",
                    "1417 Elm St",
                    "1519 Fir St",
                    "1619 Elm St",
                ],
            ),
            (
                Columns.PHONE,
                [
                    "+1 360-555-1234",
                    "+1 360-555-5678",
                    "+1 360-555-9012",
                    "+1 360-555-3456",
                    "+1 360-555-7890",
                    "",
                    "",
                    "",
                    "+1 360-555-1001",
                ],
            ),
            (
                Columns.EMAIL,
                [
                    "me@me.com",
                    "you@me.com",
                    "we@me.com",
                    "me@you.com",
                    "you@you.com",
                    "we@you.com",
                    "me@we.com",
                    "you@we.com",
                    "we@we.com",
                ],
            ),
            (Columns.NOTES, ["", "Drop the box.", "", "", "", "", "", "", ""]),
            (Columns.ORDER_COUNT, [1, 1, 1, 1, MAX_ORDER_COUNT, 1, 1, 1, 1]),
            (
                Columns.BOX_TYPE,
                [
                    "BASIC",
                    "BASIC",
                    "BASIC",
                    "BASIC",
                    "BASIC",
                    "BASIC",
                    "BASIC",
                    "BASIC",
                    "BASIC",
                ],
            ),
            (
                Columns.NEIGHBORHOOD,
                ["YORK", "YORK", "YORK", "YORK", "YORK", "YORK", "YORK", "YORK", "YORK"],
            ),
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
                    "+13605551234",
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
                    " +13605555678 ",  # Test stripping whitespace.
                    "you@me.com ",  # Test stripping whitespace.
                    " Drop the box.",  # Test stripping whitespace.
                    "1 ",  # Test stripping whitespace.
                    " Basic ",  # Test stripping whitespace.
                    " York",  # Test stripping whitespace.
                ),
                (
                    "Boaty McBoatface",  # Test real name.
                    3.0,  # Test cast float.
                    "Charlie Bucket",  # Test real name.
                    "789 Oak St",
                    "13605559012",  # Without +.
                    # TODO: File issue with email_validator.
                    # Examples claim spaces are removed from domains.
                    "we@mE.cOm",  # Test domain case formatting.
                    "",
                    1.0,  # Test cast float.
                    "Basic",
                    "York",
                ),
                (
                    "Tim #2",  # Test special character and numbers.
                    "4.0 ",  # Test cast str float.
                    "David & Jonathan",  # Test special character.
                    "1011 Pine St",
                    13605553456,  # Test cast from int.
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
                    "+1 (360) 555-7890",  # Test U.S. standard format.
                    "you@you.com",
                    "",
                    MAX_ORDER_COUNT,  # Test max order count.
                    "Basic",
                    "York",
                ),
                (
                    "Driver",
                    6,
                    "Frank",
                    "1315 Birch St",
                    "",  # Test empty string.
                    "we@you.com",
                    "",
                    1,
                    "Basic",
                    "York",
                ),
                (
                    "Driver",
                    7,
                    "Gina",
                    "1417 Elm St",
                    None,  # Test null.
                    "me@we.com",
                    "",
                    1,
                    "Basic",
                    "York",
                ),
                (
                    "Driver",
                    8,
                    "Hank",
                    "1519 Fir St",
                    " ",  # Test empty white space.
                    "you@we.com",
                    "",
                    1,
                    "Basic",
                    "York",
                ),
                (
                    "Driver",
                    9,
                    "Ivy",
                    "1619 Elm St",
                    13605551001.0,  # Test float.
                    "we@we.com",
                    "",
                    1,
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
                pytest.warns(
                    UserWarning,
                    match=re.escape(
                        "Invalid email addresses found: ['us@them..com', 'u@s@them.com']"
                    ),
                ),
                format_and_validate_data,
            ),
            (
                pd.DataFrame(
                    {
                        Columns.PHONE: INVALID_NUMBERS
                        + [  # noqa: W503
                            "+1"
                            + str(  # noqa: W503
                                phonenumbers.example_number(region_code="US").national_number
                            )
                        ]
                    }
                ),
                pytest.warns(
                    UserWarning, match=f"Invalid phone numbers found: {INVALID_NUMBERS}"
                ),
                format_and_validate_data,
            ),
            (pd.DataFrame({Columns.EMAIL: [""]}), nullcontext(), format_and_validate_data),
            (pd.DataFrame({Columns.PHONE: [""]}), nullcontext(), format_and_validate_data),
            (
                pd.DataFrame({Columns.ADDRESS: [None]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.ADDRESS: [""]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.BOX_TYPE: [None]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.BOX_TYPE: [""]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.BOX_TYPE: [None]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
                format_and_validate_data,
            ),
            (
                pd.DataFrame({Columns.BOX_TYPE: [""]}),
                pytest.raises(ValueError),  # Actually, throws error when casting to string.
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
            if validating_function == format_and_validate_data:
                format_and_validate_data(df=df, columns=df.columns.to_list())
            else:
                validating_function(df=df)
