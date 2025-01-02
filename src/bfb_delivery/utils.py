"""Utility functions for the BFB Delivery project."""

import configparser
import os
import warnings
from pathlib import Path

import pandas as pd
from typeguard import typechecked


# TODO: Typecheck.
def get_phone_number(key: str, config_path: str = "config.ini") -> str:
    """Get the phone number from the config file.

    Args:
        key: The key in the config file.
        config_path: The path to the config file.

    Returns:
        The phone number.
    """
    section_key = "phone_numbers"
    phone_number = (
        "NO PHONE NUMBER. "
        "See warning in logs for instructions on setting up your config file."
    )
    full_config_path = Path(config_path)
    full_config_path = full_config_path.resolve()
    config_instructions = (
        f"In config file, under '[{section_key}]', add '{key} = (555) 555-5555'."
    )

    if os.path.exists(config_path):
        config = configparser.ConfigParser()
        try:
            config.read(full_config_path)
            phone_number = config[section_key][key]
        except KeyError:
            warnings.warn(
                f"{key} not found in config file: {full_config_path}. {config_instructions}",
                stacklevel=2,
            )
    else:
        warnings.warn(
            (
                f"Config file not found: {full_config_path}. "
                f"Create the file. {config_instructions}"
            ),
            stacklevel=2,
        )

    return str(phone_number)


@typechecked
def map_columns(df: pd.DataFrame, column_name_map: dict[str, str], invert_map: bool) -> None:
    """Map column names in a DataFrame.

    Operates in place.

    Args:
        df: The DataFrame to map.
        column_name_map: The mapping of column names.
        invert_map: Whether to invert the mapping.
    """
    if invert_map:
        column_name_map = {v: k for k, v in column_name_map.items()}

    df.rename(columns=column_name_map, inplace=True)
