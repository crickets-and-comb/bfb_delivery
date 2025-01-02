"""Unit tests for the utility functions."""

from contextlib import AbstractContextManager, nullcontext

import pandas as pd
import pytest

from bfb_delivery.utils import get_phone_number, map_columns


@pytest.mark.parametrize(
    "key, config_path, expected, expected_warning",
    [
        ("driver_support", "config.ini", "555-555-5555", nullcontext()),
        ("recipient_support", "config.ini", "555-555-5555 x5", nullcontext()),
        (
            "bad_key",
            "config.ini",
            (
                "NO PHONE NUMBER. See warning in logs for instructions on setting up your "
                "config file."
            ),
            pytest.warns(UserWarning, match="bad_key not found in config file: "),
        ),
        ("driver_support", None, "555-555-5555", nullcontext()),
        ("recipient_support", None, "555-555-5555 x5", nullcontext()),
        (
            "bad_key",
            None,
            (
                "NO PHONE NUMBER. See warning in logs for instructions on setting up your "
                "config file."
            ),
            pytest.warns(UserWarning, match="bad_key not found in config file: "),
        ),
        (
            "driver_support",
            "bad_config.ini",
            (
                "NO PHONE NUMBER. See warning in logs for instructions on setting up your "
                "config file."
            ),
            pytest.warns(UserWarning, match="Config file not found: "),
        ),
        (
            "recipient_support",
            "bad_config.ini",
            (
                "NO PHONE NUMBER. See warning in logs for instructions on setting up your "
                "config file."
            ),
            pytest.warns(UserWarning, match="Config file not found: "),
        ),
        (
            "bad_key",
            "bad_config.ini",
            (
                "NO PHONE NUMBER. See warning in logs for instructions on setting up your "
                "config file."
            ),
            pytest.warns(UserWarning, match="Config file not found: "),
        ),
    ],
)
def test_get_phone_number(
    key: str, config_path: None | str, expected: str, expected_warning: AbstractContextManager
) -> None:
    """Correct phone number returned, and warns on no config file or missing key."""
    kwargs = {"key": key}
    if config_path is not None:
        kwargs["config_path"] = config_path
    with expected_warning:
        returned_phone_number = get_phone_number(**kwargs)
    assert returned_phone_number == expected


@pytest.mark.parametrize("invert_map", [False, True])
def test_map_columns(invert_map: bool) -> None:
    """Test that the columns are mapped correctly."""
    df = pd.DataFrame(
        {
            "Box Type": ["BASIC", "GF", "LA"],
            "Box Count": [1, 2, 3],
            "Driver": ["John", "Jane", "Jim"],
        }
    )
    column_name_map = {"Box Type": "Product Type"}
    map_columns(df, column_name_map, invert_map=invert_map)
    if not invert_map:
        assert df.columns.to_list() == ["Product Type", "Box Count", "Driver"]
    else:
        assert df.columns.to_list() == ["Box Type", "Box Count", "Driver"]
