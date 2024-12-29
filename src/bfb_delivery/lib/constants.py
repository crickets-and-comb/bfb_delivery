"""Constants used in the project."""

from typing import Final


class CellColors:
    """Colors for spreadsheet formatting."""

    BASIC: Final[str] = "FFCC00"  # Orange
    HEADER: Final[str] = "FFCCCC"  # Pink
    LA: Final[str] = "3399CC"  # Blue
    GF: Final[str] = "99CC33"  # Green
    VEGAN: Final[str] = "CCCCCC"  # Grey


# TODO: Make box type StrEnum.
BOX_TYPE_COLOR_MAP: Final[dict[str, str]] = {
    "BASIC": CellColors.BASIC,
    "GF": CellColors.GF,
    "LA": CellColors.LA,
    "VEGAN": CellColors.VEGAN,
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
