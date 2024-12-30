"""Constants used in the project."""

from enum import StrEnum
from typing import Final


class BoxType(StrEnum):
    """Box types for the delivery service."""

    BASIC = "BASIC"
    GF = "GF"
    LA = "LA"
    VEGAN = "VEGAN"


class CellColors:
    """Colors for spreadsheet formatting."""

    BASIC: Final[str] = "00FFCC00"  # Orange
    HEADER: Final[str] = "00FFCCCC"  # Pink
    LA: Final[str] = "003399CC"  # Blue
    GF: Final[str] = "0099CC33"  # Green
    VEGAN: Final[str] = "00CCCCCC"  # Grey


# TODO: Make box type StrEnum.
BOX_TYPE_COLOR_MAP: Final[dict[str, str]] = {
    BoxType.BASIC: CellColors.BASIC,
    BoxType.GF: CellColors.GF,
    BoxType.LA: CellColors.LA,
    BoxType.VEGAN: CellColors.VEGAN,
}


class Columns:
    """Column name constants."""

    ADDRESS: Final[str] = "Address"
    BOX_TYPE: Final[str] = "Box Type"
    BOX_COUNT: Final[str] = "Box Count"
    DRIVER: Final[str] = "Driver"  # TODO: Accept any case of columns?
    EMAIL: Final[str] = "Email"
    NAME: Final[str] = "Name"
    NEIGHBORHOOD: Final[str] = "Neighborhood"
    NOTES: Final[str] = "Notes"
    ORDER_COUNT: Final[str] = "Order Count"
    PHONE: Final[str] = "Phone"
    STOP_NO: Final[str] = "Stop #"


COMBINED_ROUTES_COLUMNS: Final[list[str]] = [
    Columns.STOP_NO,
    Columns.NAME,
    Columns.ADDRESS,
    Columns.PHONE,
    Columns.NOTES,
    Columns.ORDER_COUNT,
    Columns.BOX_TYPE,
    Columns.NEIGHBORHOOD,
]

FILE_DATE_FORMAT: Final[str] = "%Y%m%d"

FORMATTED_ROUTES_COLUMNS: Final[list[str]] = [
    Columns.STOP_NO,
    Columns.NAME,
    Columns.ADDRESS,
    Columns.PHONE,
    Columns.NOTES,
    Columns.BOX_TYPE,
]

MANIFEST_DATE_FORMAT: Final[str] = "%m.%d"

MAX_ORDER_COUNT: Final[int] = 5

NOTES_COLUMN_WIDTH: Final[float] = 56.67

PROTEIN_BOX_TYPES: Final[list[str]] = ["BASIC", "GF", "LA"]

SPLIT_ROUTE_COLUMNS: Final[list[str]] = [
    Columns.NAME,
    Columns.ADDRESS,
    Columns.PHONE,
    Columns.EMAIL,
    Columns.NOTES,
    Columns.ORDER_COUNT,
    Columns.BOX_TYPE,
    Columns.NEIGHBORHOOD,
]
