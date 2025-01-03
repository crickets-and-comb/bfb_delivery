"""Constants used in the project."""

from enum import StrEnum
from typing import Final


class BoxType(StrEnum):
    """Box types for the delivery service."""

    BASIC = "BASIC"
    GF = "GF"
    LA = "LA"
    VEGAN = "VEGAN"


class CellColors:  # TODO: Use accessible palette.
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


# TODO: Make StrEnum.
class Columns:
    """Column name constants."""

    ADDRESS: Final[str] = "Address"
    BOX_TYPE: Final[str] = "Box Type"
    BOX_COUNT: Final[str] = "Box Count"
    DRIVER: Final[str] = "Driver"
    EMAIL: Final[str] = "Email"
    NAME: Final[str] = "Name"
    NEIGHBORHOOD: Final[str] = "Neighborhood"
    NOTES: Final[str] = "Notes"
    ORDER_COUNT: Final[str] = "Order Count"
    PHONE: Final[str] = "Phone"
    PRODUCT_TYPE: Final[str] = "Product Type"
    STOP_NO: Final[str] = "Stop #"


COLUMN_NAME_MAP: Final[dict[str, str]] = {Columns.BOX_TYPE: Columns.PRODUCT_TYPE}


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


class Defaults:
    """Default values. E.g., for syncing public API with CLI."""

    COMBINE_ROUTE_TABLES: Final[dict[str, str]] = {"output_dir": "", "output_filename": ""}
    FORMAT_COMBINED_ROUTES: Final[dict[str, str]] = {
        "output_dir": "",
        "output_filename": "",
        "date": "",
    }
    SPLIT_CHUNKED_ROUTE: Final[dict[str, str | int]] = {
        "output_dir": "",
        "output_filename": "",
        "n_books": 4,
    }


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
    Columns.PRODUCT_TYPE,
    Columns.NEIGHBORHOOD,
]
