"""Data cleaning utilities."""

from collections.abc import Callable

import pandas as pd
from typeguard import typechecked

from bfb_delivery.lib.constants import MAX_ORDER_COUNT, Columns


@typechecked
def format_column_names(columns: list[str]) -> list[str]:
    """Clean column names.

    Just strips whitespace for now.

    Args:
        columns: The column names to clean.

    Returns:
        The cleaned column names.
    """
    # TODO: Other column cleaning? (e.g., remove special characters, set casing)
    columns = [column.strip() for column in columns]
    # TODO: Validate? Use general constant list?
    return columns


def format_and_validate_data(df: pd.DataFrame, columns: list[str]) -> None:
    """Clean, format, and validate selected columns in a DataFrame.

    Operates in place.

    Args:
        df: The DataFrame to clean.
        columns: The columns to clean.

    Returns:
        None

    Raises:
        ValueError: If columns are not found in the DataFrame.
        ValueError: If no formatter is found for a column.
    """
    missing_columns = sorted(list(set(columns) - set(df.columns)))
    if missing_columns:
        raise ValueError(f"Columns not found in DataFrame: {missing_columns}.")

    # TODO: Pre-Validate:
    # ints actually integers and not something that gets cast to an int (beautfulsoup?)

    # TODO: Could use generic or class? But, this works, and is flexible and transparent.
    # TODO: Could remove smurf typing (_column), but wait to see if using lambdas etc.
    formatters_dict = {
        Columns.ADDRESS: _format_address_column,
        Columns.BOX_TYPE: _format_box_type_column,
        Columns.EMAIL: _format_email_column,
        Columns.DRIVER: _format_driver_column,
        Columns.NAME: _format_name_column,
        Columns.NEIGHBORHOOD: _format_neighborhood_column,
        Columns.NOTES: _format_notes_column,
        Columns.ORDER_COUNT: _format_order_count_column,
        Columns.PHONE: _format_phone_column,
        Columns.STOP_NO: _format_stop_no_column,
    }
    for column in columns:
        formatter_fx: Callable
        try:
            formatter_fx = formatters_dict[column]
        except KeyError as e:
            raise ValueError(f"No formatter found for column: {column}.") from e
        formatter_fx(df=df)

    # TODO: Sort by driver and stop number if avaailable.

    # TODO: Split validation into second step so we can test validations.

    return


# TODO: Some common (post-formatting) validations for all columns:
# Are the prescribed types.
# Have no nulls (where appropriate).

# TODO: Some common (post-formatting) validations for column types:
# int: > 0


def _format_address_column(df: pd.DataFrame) -> None:
    """Format the address column."""
    _format_string_column(df=df, column=Columns.ADDRESS)
    # TODO: Other formatting? beautifulsoup?
    # TODO: Validate: Use beautifulsoup or something?
    return


def _format_box_type_column(df: pd.DataFrame) -> None:
    """Format the box type column."""
    _format_string_column(df=df, column=Columns.BOX_TYPE)
    # TODO: What about multiple box types for one stop?
    # Split and format each value separately, then rejoin.
    # TODO: Validate: make enum.StrEnum?
    return


def _format_email_column(df: pd.DataFrame) -> None:
    """Format the email column."""
    _format_string_column(df=df, column=Columns.EMAIL)
    # TODO: Other formatting? beautifulsoup?
    # TODO: Validate: Use beautifulsoup or something?
    return


# TODO: Make this wrap a list formatter to use that for sheet names.
def _format_driver_column(df: pd.DataFrame) -> None:
    """Format the driver column."""
    _format_string_column(df=df, column=Columns.DRIVER)
    # TODO: See name formatter to include anything added there.
    # TODO: Abstract name formatting?
    return


def _format_name_column(df: pd.DataFrame) -> None:
    """Format the name column."""
    _format_string_column(df=df, column=Columns.NAME)
    # TODO: Vaidate:
    # no special characters
    # no numbers?
    # beautifulsoup?
    return


def _format_neighborhood_column(df: pd.DataFrame) -> None:
    """Format the neighborhood column."""
    _format_string_column(df=df, column=Columns.NEIGHBORHOOD)
    # TODO: Validate: make enum.StrEnum?
    return


def _format_notes_column(df: pd.DataFrame) -> None:
    """Format the notes column."""
    _format_string_column(df=df, column=Columns.NOTES)
    return


def _format_order_count_column(df: pd.DataFrame) -> None:
    """Format the order count column."""
    _format_int_column(df=df, column=Columns.ORDER_COUNT)
    _validate_order_count_column(df=df)
    return


def _format_phone_column(df: pd.DataFrame) -> None:
    """Format the phone column."""
    _format_string_column(df=df, column=Columns.PHONE)
    # TODO: Other formatting? beautifulsoup?
    # area and country code.
    # different input formats (e.g., dashes, parentheses, spaces, periods)
    # TODO: Validate: Use beautifulsoup or something?
    return


def _format_stop_no_column(df: pd.DataFrame) -> None:
    """Format the stop number column."""
    _format_int_column(df=df, column=Columns.STOP_NO)
    _validate_stop_no_column(df=df)
    return


def _format_int_column(df: pd.DataFrame, column: str) -> None:
    """Basic formatting for an integer column."""
    _strip_whitespace_from_column(df=df, column=column)
    df[column] = df[column].astype(float).astype(int)
    return


def _format_string_column(df: pd.DataFrame, column: str) -> None:
    """Basic formatting for a string column."""
    _strip_whitespace_from_column(df=df, column=column)
    # TODO: Other formatting? (e.g., remove special characters)
    return


def _strip_whitespace_from_column(df: pd.DataFrame, column: str) -> None:
    """Strip whitespace from a column. Note: Casts to string."""
    df[column] = df[column].astype(str).str.strip()
    return


def _validate_order_count_column(df: pd.DataFrame) -> None:
    """Validate the order count column."""
    _validate_col_not_empty(df=df, column=Columns.ORDER_COUNT)
    _validate_greater_than_zero(df=df, column=Columns.ORDER_COUNT)

    too_many_orders_df = df[df[Columns.ORDER_COUNT] > MAX_ORDER_COUNT]
    if not too_many_orders_df.empty:
        raise ValueError(
            f"Order count exceeds maximum of {MAX_ORDER_COUNT}: " f"{too_many_orders_df}"
        )

    return


def _validate_stop_no_column(df: pd.DataFrame) -> None:
    """Validate the stop number column."""
    _validate_col_not_empty(df=df, column=Columns.STOP_NO)
    _validate_greater_than_zero(df=df, column=Columns.STOP_NO)

    duplicates_df = df[df.duplicated(subset=[Columns.STOP_NO], keep=False)]
    if not duplicates_df.empty:
        raise ValueError(f"Duplicate stop numbers found: {duplicates_df}")

    stop_numbers = df[Columns.STOP_NO].to_list()
    if sorted(stop_numbers) != list(range(1, len(stop_numbers) + 1)):
        raise ValueError(f"Stop numbers are not contiguous starting at 1: {stop_numbers}")

    if stop_numbers != sorted(stop_numbers):
        raise ValueError(f"Stop numbers are not sorted: {stop_numbers}")

    return


def _validate_col_not_empty(df: pd.DataFrame, column: str) -> None:
    """No nulls or empty strings in column."""
    null_df = df[df[column].isnull()]
    if not null_df.empty:
        raise ValueError(f"Null values found in {column} column: " f"{null_df}")

    empty_df = df[df[column] == ""]
    if not empty_df.empty:
        raise ValueError(f"Empty values found in {column} column: " f"{empty_df}")

    return


def _validate_greater_than_zero(df: pd.DataFrame, column: str) -> None:
    """Validate column is greater than zero."""
    negative_df = df[df[column] <= 0]
    if not negative_df.empty:
        raise ValueError(
            f"Values less than or equal to zero found in {column} column: " f"{negative_df}"
        )

    return
