"""Data cleaning utilities."""

import warnings
from collections.abc import Callable
from logging import info

import email_validator
import pandas as pd
import phonenumbers
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
    columns = [column.strip() for column in columns]
    # TODO: Other column cleaning? (e.g., remove special characters, set casing)
    # TODO: Validate? Use general constant list?
    # TODO: Make column names StrEnum? Or just make sure they are in the constants list?
    return columns


@typechecked
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

    # TODO: FutureWarning: Setting an item of incompatible dtype is deprecated and will
    # raise an error in a future version of pandas. Value '' has dtype incompatible with
    # float64, please explicitly cast to a compatible dtype first.
    # Cast all to string to start? Then convert "nan" to ""?
    df.fillna("", inplace=True)

    # Could use generic, class, or Pandera? But, this works, and is flexible and transparent.
    formatters_dict = {
        Columns.ADDRESS: _format_and_validate_address,
        Columns.BOX_TYPE: _format_and_validate_box_type,
        Columns.EMAIL: _format_and_validate_email,
        Columns.DRIVER: _format_and_validate_driver,
        Columns.NAME: _format_and_validate_name,
        Columns.NEIGHBORHOOD: _format_and_validate_neighborhood,
        Columns.NOTES: _format_notes,
        Columns.ORDER_COUNT: _format_and_validate_order_count,
        Columns.PHONE: _format_and_validate_phone,
        Columns.STOP_NO: _format_and_validate_stop_no,
    }
    for column in columns:
        formatter_fx: Callable
        try:
            formatter_fx = formatters_dict[column]
        except KeyError as e:
            raise ValueError(f"No formatter found for column: {column}.") from e
        formatter_fx(df=df)

    # TODO: Some common (post-formatting) validations for all columns:
    # Are the prescribed types.
    # Have no nulls (where appropriate).

    return


@typechecked
def _format_and_validate_address(df: pd.DataFrame) -> None:
    """Format the address column."""
    # Avoid modifying values until we need to. Mostly established values used in Circuit.
    # Will hold off on validation/formatting until we've swallowed more of the process
    # and are starting to map etc.
    _format_string(df=df, column=Columns.ADDRESS)
    _validate_col_not_empty(df=df, column=Columns.ADDRESS)
    return


@typechecked
def _format_and_validate_box_type(df: pd.DataFrame) -> None:
    """Format the box type column."""
    _format_and_validate_names(df=df, column=Columns.BOX_TYPE)
    # TODO: What about multiple box types for one stop?
    # Split and format each value separately, then rejoin?
    # TODO: Validate: make enum.StrEnum?
    return


@typechecked
def _format_and_validate_driver(df: pd.DataFrame) -> None:
    """Format the driver column."""
    _format_and_validate_names(df=df, column=Columns.DRIVER)
    return


@typechecked
def _format_and_validate_email(df: pd.DataFrame) -> None:
    """Format and validate the email column."""
    _format_string(df=df, column=Columns.EMAIL)

    formatted_emails = []
    invalid_emails = []
    for email in df[Columns.EMAIL]:
        formatted_email = email

        try:
            if email:
                email_info = email_validator.validate_email(email, check_deliverability=False)
                formatted_email = email_info.normalized
        except email_validator.EmailNotValidError as e:
            invalid_emails.append(email)
            warnings.warn(message=f"Invalid email address, {email}: {e}", stacklevel=2)
            info("Checking for more invalid addresses before raising error.")

        formatted_emails.append(formatted_email)

    if invalid_emails:
        warnings.warn(
            message=f"Invalid email addresses found: {invalid_emails}", stacklevel=2
        )
    else:
        df[Columns.EMAIL] = formatted_emails

    return


@typechecked
def _format_and_validate_name(df: pd.DataFrame) -> None:
    """Format the name column."""
    _format_and_validate_names(df=df, column=Columns.NAME)
    return


@typechecked
def _format_and_validate_neighborhood(df: pd.DataFrame) -> None:
    """Format the neighborhood column."""
    _format_and_validate_names(df=df, column=Columns.NEIGHBORHOOD)
    # TODO: Validate: make enum.StrEnum?
    return


@typechecked
def _format_notes(df: pd.DataFrame) -> None:
    """Format the notes column."""
    _format_string(df=df, column=Columns.NOTES)
    return


@typechecked
def _format_and_validate_order_count(df: pd.DataFrame) -> None:
    """Format the order count column."""
    _format_int(df=df, column=Columns.ORDER_COUNT)
    _validate_order_count(df=df)
    return


@typechecked
def _format_and_validate_phone(df: pd.DataFrame) -> None:
    """Format and validate the phone column."""
    _format_string(df=df, column=Columns.PHONE)
    df[Columns.PHONE] = df[Columns.PHONE].apply(lambda x: x[:-2] if x.endswith(".0") else x)

    validation_df = df.copy()
    validation_df["formatted_numbers"] = validation_df[Columns.PHONE].apply(
        lambda number: "+" + number if (len(number) > 0 and number[0] != "+") else number
    )
    validation_df["formatted_numbers"] = [
        phonenumbers.parse(number) if len(number) > 0 else number
        for number in validation_df["formatted_numbers"].to_list()
    ]

    validation_df["is_valid"] = validation_df["formatted_numbers"].apply(
        lambda number: (
            phonenumbers.is_valid_number(number)
            if isinstance(number, phonenumbers.phonenumber.PhoneNumber)
            else True
        )
    )
    if not validation_df["is_valid"].all():
        invalid_numbers = validation_df[~validation_df["is_valid"]]
        warnings.warn(
            message=f"Invalid phone numbers found: {invalid_numbers[df.columns.to_list()]}",
            stacklevel=2,
        )

    # TODO: Use phonenumbers.format_by_pattern to achieve (555) 555-5555 if desired.
    validation_df["formatted_numbers"] = [
        (
            str(
                phonenumbers.format_number(
                    number, num_format=phonenumbers.PhoneNumberFormat.INTERNATIONAL
                )
            )
            if isinstance(number, phonenumbers.phonenumber.PhoneNumber)
            else number
        )
        for number in validation_df["formatted_numbers"].to_list()
    ]

    df[Columns.PHONE] = validation_df["formatted_numbers"]

    return


@typechecked
def _format_and_validate_stop_no(df: pd.DataFrame) -> None:
    """Format the stop number column."""
    _format_int(df=df, column=Columns.STOP_NO)
    _validate_stop_no(df=df)
    return


@typechecked
def _format_and_validate_names(df: pd.DataFrame, column: str) -> None:
    """Format a column with names."""
    _format_string(df=df, column=column)
    _validate_col_not_empty(df=df, column=column)
    # Could use nameparser or str.title(), but neither handles all cases. Some other package?
    df[column] = df[column].apply(lambda name: name.upper())
    return


@typechecked
def _format_int(df: pd.DataFrame, column: str) -> None:
    """Basic formatting for an integer column."""
    df[column] = df[column].astype(str).str.strip()
    df[column] = df[column].astype(float).astype(int)
    return


@typechecked
def _format_string(df: pd.DataFrame, column: str) -> None:
    """Basic formatting for a string column. Note: Casts to string."""
    df[column] = df[column].astype(str).str.strip()
    return


@typechecked
def _validate_order_count(df: pd.DataFrame) -> None:
    """Validate the order count column."""
    _validate_col_not_empty(df=df, column=Columns.ORDER_COUNT)
    _validate_greater_than_zero(df=df, column=Columns.ORDER_COUNT)

    too_many_orders_df = df[df[Columns.ORDER_COUNT] > MAX_ORDER_COUNT]
    if not too_many_orders_df.empty:
        raise ValueError(
            f"Order count exceeds maximum of {MAX_ORDER_COUNT}: " f"{too_many_orders_df}"
        )

    return


@typechecked
def _validate_stop_no(df: pd.DataFrame) -> None:
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


@typechecked
def _validate_col_not_empty(df: pd.DataFrame, column: str) -> None:
    """No nulls or empty strings in column."""
    null_df = df[df[column].isnull()]
    if not null_df.empty:
        raise ValueError(f"Null values found in {column} column: " f"{null_df}")

    empty_df = df[df[column] == ""]
    if not empty_df.empty:
        raise ValueError(f"Empty values found in {column} column: " f"{empty_df}")

    return


@typechecked
def _validate_greater_than_zero(df: pd.DataFrame, column: str) -> None:
    """Validate column is greater than zero."""
    negative_df = df[df[column] <= 0]
    if not negative_df.empty:
        raise ValueError(
            f"Values less than or equal to zero found in {column} column: " f"{negative_df}"
        )

    return
