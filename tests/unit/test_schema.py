"""Tests for the schema module."""

import pandera.pandas as pa
import pytest
from typeguard import typechecked

from bfb_delivery.lib.constants import IntermediateColumns, SpokeColumns
from bfb_delivery.lib.schema import (
    PlansAssignDriversIn,
    SpokePlansFromDict,
    SpokePlansOut,
    SpokePlansTransformIn,
    SpokeRoutesTransformOut,
    SpokeRoutesWriteIn,
    Stops,
)

_TWO_WORDS_CHECK = "at_least_two_words"


@typechecked
def _check_names(model: type[pa.DataFrameModel], column: str) -> list[str]:
    return [check.name for check in model.to_schema().columns[column].checks]


@pytest.mark.parametrize(
    "model, column",
    [
        (Stops, IntermediateColumns.SHEET_NAME),
        (PlansAssignDriversIn, IntermediateColumns.ROUTE_TITLE),
    ],
)
@typechecked
def test_upload_titles_require_two_words(model: type[pa.DataFrameModel], column: str) -> None:
    """Titles we upload to Spoke must follow the "date driver" naming convention."""
    assert _TWO_WORDS_CHECK in _check_names(model=model, column=column)


@pytest.mark.parametrize(
    "model, column",
    [
        (SpokePlansOut, SpokeColumns.TITLE),
        (SpokePlansFromDict, SpokeColumns.TITLE),
        (SpokePlansTransformIn, SpokeColumns.TITLE),
        (SpokeRoutesTransformOut, IntermediateColumns.DRIVER_SHEET_NAME),
        (SpokeRoutesTransformOut, IntermediateColumns.ROUTE_TITLE),
        (SpokeRoutesWriteIn, IntermediateColumns.DRIVER_SHEET_NAME),
    ],
)
@typechecked
def test_download_titles_allow_one_word(model: type[pa.DataFrameModel], column: str) -> None:
    """Titles we download from Spoke need not follow the naming convention.

    https://github.com/crickets-and-comb/bfb_delivery/issues/143
    """
    check_names = _check_names(model=model, column=column)
    assert _TWO_WORDS_CHECK not in check_names
    # Still must be non-blank, since titles become sheet and file names.
    assert "str_matches" in check_names
