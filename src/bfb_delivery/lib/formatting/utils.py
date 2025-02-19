"""Utility functions for the formatting module."""

import configparser
import logging
import os
from pathlib import Path

import pandas as pd
from typeguard import typechecked

from bfb_delivery.lib.constants import BookOneDrivers, Columns, ExtraNotes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@typechecked
def get_book_one_drivers(file_path: str) -> list[str]:
    """Get the drivers from the book-one driver's file, or the constant if no path.

    Args:
        file_path: Path to the book-one driver's file. If empty, uses a constant list.

    Returns:
        The drivers to include in book one of split chunked routes.
    """
    sheet_one_drivers = [d.value for d in BookOneDrivers]
    if file_path:
        sheet_one_drivers = pd.read_csv(file_path)[Columns.DRIVER].astype(dtype=str).tolist()

    return sheet_one_drivers


@typechecked
def get_extra_notes(file_path: str) -> pd.DataFrame:
    """Get the extra notes from the file, or the constant if no path.

    Args:
        file_path: Path to the extra notes file. If empty, uses a constant DataFrame.

    Returns:
        The extra notes to include in the combined routes.
    """
    extra_notes_df: pd.DataFrame
    if file_path:
        extra_notes_df = pd.read_csv(file_path)
    else:
        extra_notes = ExtraNotes()
        extra_notes_df = extra_notes.df

    validation_sr = extra_notes_df["tag"]
    validation_sr = validation_sr.apply(lambda x: str(x).replace("*", "").strip())
    duplicated_tags = validation_sr[validation_sr.duplicated()].to_list()
    if duplicated_tags:
        raise ValueError(f"Extra notes has duplicated tags: {duplicated_tags}")

    return extra_notes_df


@typechecked
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

    if os.path.exists(full_config_path):
        config = configparser.ConfigParser()
        try:
            config.read(full_config_path)
            phone_number = config[section_key][key]
        except KeyError:
            logger.warning(
                f"{key} not found in config file: {full_config_path}. {config_instructions}"
            )
    else:
        logger.warning(
            (
                f"Config file not found: {full_config_path}. "
                f"Create the file. {config_instructions}"
            )
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
