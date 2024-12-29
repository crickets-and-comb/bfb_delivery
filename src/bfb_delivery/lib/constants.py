"""Constants used in the project."""

# TODO: Make enums? (Use Pandera?): box types, neighborhood

from typing import Final


class CellColors:
    """Colors for spreadsheet formatting."""

    BASIC: Final[str] = "FFA500"  # Orange
    HEADER: Final[str] = "FFC0CB"  # Pink
    LA: Final[str] = "0000FF"  # Blue
    GF: Final[str] = "008000"  # Green
    VEGAN: Final[str] = "808080"  # Grey


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

FORMATTED_ROUTES_COLUMNS: Final[list[str]] = [
    Columns.STOP_NO,
    Columns.NAME,
    Columns.ADDRESS,
    Columns.PHONE,
    Columns.NOTES,
    Columns.BOX_TYPE,
]

MAX_ORDER_COUNT: Final[int] = 5

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
