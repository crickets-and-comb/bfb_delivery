"""Unit tests for the utility functions."""

import re
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Final
from unittest.mock import patch

import pandas as pd
import pytest

from bfb_delivery.lib.formatting.utils import get_extra_notes, get_phone_number, map_columns


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


@pytest.mark.parametrize(
    "extra_notes_df, error_context",
    [
        (
            pd.DataFrame(
                columns=["tag", "note"],
                data=[("tag1", "note1"), ("tag2", "note2"), ("tag3", "note3")],
            ),
            nullcontext(),
        ),
        (
            pd.DataFrame(
                columns=["tag", "note"],
                data=[("tag1", "note1"), ("tag2", "note2"), ("tag1", "note3")],
            ),
            pytest.raises(
                ValueError, match=re.escape(("Extra notes has duplicated tags: ['tag1']"))
            ),
        ),
        (
            pd.DataFrame(
                columns=["tag", "note"],
                data=[("tag1*", "note1"), ("tag2", "note2"), ("tag1 *", "note3")],
            ),
            pytest.raises(
                ValueError, match=re.escape(("Extra notes has duplicated tags: ['tag1']"))
            ),
        ),
    ],
)
@pytest.mark.parametrize("file_name", ["extra_notes.csv", ""])
def test_get_extra_notes(
    extra_notes_df: pd.DataFrame,
    error_context: AbstractContextManager,
    file_name: str,
    tmp_path: Path,
) -> None:
    """Test that the extra notes are read correctly."""
    mock_extra_notes_context = nullcontext()
    file_path = str(tmp_path / file_name) if file_name else file_name

    if file_name:
        extra_notes_df.to_csv(file_path, index=False)
    else:

        class TestExtraNotes:
            df: Final[pd.DataFrame] = extra_notes_df

        mock_extra_notes_context = patch(
            "bfb_delivery.lib.formatting.utils.ExtraNotes", new=TestExtraNotes
        )

    with error_context, mock_extra_notes_context:
        returned_extra_notes_df = get_extra_notes(file_path=file_path)
        assert returned_extra_notes_df.equals(extra_notes_df)
