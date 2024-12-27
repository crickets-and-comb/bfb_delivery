"""Data cleaning utilities."""

from collections.abc import Callable

import pandas as pd
from typeguard import typechecked

from bfb_delivery.lib.constants import Columns


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

    # TODO: Could use generic or class? But, this works, and is flexible and transparent.
    formatters_dict = {
        Columns.STOP_NO: _format_stop_no_column,
        Columns.NAME: _format_name_column,
        Columns.ADDRESS: _format_address_column,
        Columns.PHONE: _format_phone_column,
        Columns.EMAIL: _format_email_column,
        Columns.NOTES: _format_notes_column,
        Columns.ORDER_COUNT: _format_order_count_column,
        Columns.BOX_TYPE: _format_box_type_column,
        Columns.NEIGHBORHOOD: _format_neighborhood_column,
    }
    for column in columns:
        formatter_fx: Callable
        try:
            formatter_fx = formatters_dict[column]
        except KeyError as e:
            raise ValueError(f"No formatter found for column: {column}.") from e
        formatter_fx(df=df)

    return


def _format_stop_no_column(df: pd.DataFrame) -> None:
    """Format the stop number column."""
    _format_int_column(df=df, column=Columns.STOP_NO)
    # TODO: Validate:
    # > 0
    # unique
    # no gaps
    # starts at 1
    # sorted
    # actually integers and not something that gets cast to an int
    # etc.?
    return


def _format_name_column(df: pd.DataFrame) -> None:
    """Format the name column."""
    _format_string_column(df=df, column=Columns.NAME)
    # TODO: Vaidate:
    # no special characters
    # no numbers?
    # beautifulsoup?
    return


def _format_address_column(df: pd.DataFrame) -> None:
    """Format the address column."""
    _format_string_column(df=df, column=Columns.ADDRESS)
    # TODO: Other formatting? beautifulsoup?
    # TODO: Validate: Use beautifulsoup or something?
    return


def _format_phone_column(df: pd.DataFrame) -> None:
    """Format the phone column."""
    _format_string_column(df=df, column=Columns.PHONE)
    # TODO: Other formatting? beautifulsoup?
    # area and country code.
    # different input formats (e.g., dashes, parentheses, spaces, periods)
    # TODO: Validate: Use beautifulsoup or something?
    return


def _format_email_column(df: pd.DataFrame) -> None:
    """Format the email column."""
    _format_string_column(df=df, column=Columns.EMAIL)
    # TODO: Other formatting? beautifulsoup?
    # TODO: Validate: Use beautifulsoup or something?
    return


def _format_notes_column(df: pd.DataFrame) -> None:
    """Format the notes column."""
    _format_string_column(df=df, column=Columns.NOTES)
    return


def _format_order_count_column(df: pd.DataFrame) -> None:
    """Format the order count column."""
    # TODO: Implement formatting.
    pass


def _format_box_type_column(df: pd.DataFrame) -> None:
    """Format the box type column."""
    # TODO: Implement formatting.
    pass


def _format_neighborhood_column(df: pd.DataFrame) -> None:
    """Format the neighborhood column."""
    # TODO: Implement formatting.
    pass


def _format_int_column(df: pd.DataFrame, column: str) -> None:
    """Basic formatting for an integer column."""
    _strip_whitespace_from_column(df=df, column=column)
    df[column] = df[column].astype(float).astype(int)
    return


def _format_string_column(df: pd.DataFrame, column: str) -> None:
    """Basic formatting for a string column."""
    _strip_whitespace_from_column(df=df, column=column)
    return


def _strip_whitespace_from_column(df: pd.DataFrame, column: str) -> None:
    """Strip whitespace from a column. Note: Casts to string."""
    df[column] = df[column].astype(str).str.strip()
    return
