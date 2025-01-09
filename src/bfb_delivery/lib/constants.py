"""Constants used in the project."""

from enum import StrEnum
from typing import Final

import pandas as pd


class BookOneDrivers(StrEnum):
    """Drivers for the first book.

    This is only an enum so it appears in docs.
    """

    YMCA = "YMCA"


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
    CREATE_MANIFESTS: Final[dict[str, str]] = {
        "output_dir": "",
        "output_filename": "",
        "date": "",
        "extra_notes_file": "",
    }
    FORMAT_COMBINED_ROUTES: Final[dict[str, str]] = {
        "output_dir": "",
        "output_filename": "",
        "date": "",
        "extra_notes_file": CREATE_MANIFESTS["extra_notes_file"],
    }
    SPLIT_CHUNKED_ROUTE: Final[dict[str, str | int]] = {
        "output_dir": "",
        "output_filename": "",
        "n_books": 4,
        "book_one_drivers_file": "",
    }


class ExtraNotes:
    """Extra notes for the combined routes.

    Is a class so it appears in docs.
    """

    notes: Final[list[tuple[str, str]]] = [
        (
            "Dummy note tag*",
            (
                "This is a dummy note. It is really long and should be so that we can "
                "test out column width and word wrapping. It should be long enough to wrap "
                "around to the next line. And, it should be long enough to wrap around to "
                "the next line. And, it should be long enough to wrap around to the next "
                "line. Hopefully, this is long enough. Also, hopefully, this is long "
                "enough. Further, hopefully, this is long enough. Additionally, it will "
                "help test out word wrapping merged cells."
            ),
        )
    ]

    df: Final[pd.DataFrame]

    def __init__(self) -> None:
        """Initialize the extra notes df."""
        self.df = pd.DataFrame(columns=["tag", "note"], data=self.notes)


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
]
