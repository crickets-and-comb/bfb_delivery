"""Constants used in the project."""

from enum import StrEnum
from typing import Final

import pandas as pd

ADDRESS_COLUMN_WIDTH: Final[float] = 40

ALL_HHS_DRIVER: Final[str] = "All HHs"


class BookOneDrivers(StrEnum):
    """Drivers for the first book.

    This is only an enum so it appears in docs.
    """

    DUMMY = "Dummy"


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


BOX_TYPE_COLOR_MAP: Final[dict[str, str]] = {
    BoxType.BASIC: CellColors.BASIC,
    BoxType.GF: CellColors.GF,
    BoxType.LA: CellColors.LA,
    BoxType.VEGAN: CellColors.VEGAN,
}


class CircuitColumns:
    """Column/field/doc name constants for Circuit API."""

    ADDRESS: Final[str] = "address"
    ADDRESS_LINE_1: Final[str] = "addressLineOne"
    ADDRESS_LINE_2: Final[str] = "addressLineTwo"
    EMAIL: Final[str] = "email"
    EXTERNAL_ID: Final[str] = "externalId"
    ID: Final[str] = "id"
    NAME: Final[str] = "name"
    NOTES: Final[str] = "notes"
    ORDER_INFO: Final[str] = "orderInfo"
    PACKAGE_COUNT: Final[str] = "packageCount"
    PHONE: Final[str] = "phone"
    PLACE_ID: Final[str] = "placeId"
    PLAN: Final[str] = "plan"
    PRODUCTS: Final[str] = "products"
    RECIPIENT: Final[str] = "recipient"
    ROUTE: Final[str] = "route"
    STOP_POSITION: Final[str] = "stopPosition"
    STOPS: Final[str] = "stops"
    TITLE: Final[str] = "title"


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

CIRCUIT_DOWNLOAD_COLUMNS: Final[list[str]] = COMBINED_ROUTES_COLUMNS + [Columns.EMAIL]


class Defaults:
    """Default values. E.g., for syncing public API with CLI."""

    COMBINE_ROUTE_TABLES: Final[dict[str, str]] = {"output_dir": "", "output_filename": ""}
    CREATE_MANIFESTS: Final[dict[str, str]] = {
        "output_dir": "",
        "output_filename": "",
        "extra_notes_file": "",
    }
    CREATE_MANIFESTS_FROM_CIRCUIT: Final[dict[str, str | bool]] = {
        "start_date": "",
        "end_date": "",
        "output_dir": CREATE_MANIFESTS["output_dir"],
        "output_filename": CREATE_MANIFESTS["output_filename"],
        "circuit_output_dir": "",
        "all_hhs": False,
        "verbose": False,
        "extra_notes_file": CREATE_MANIFESTS["extra_notes_file"],
    }
    FORMAT_COMBINED_ROUTES: Final[dict[str, str]] = {
        "output_dir": "",
        "output_filename": "",
        "extra_notes_file": CREATE_MANIFESTS["extra_notes_file"],
    }
    SPLIT_CHUNKED_ROUTE: Final[dict[str, str | int]] = {
        "output_dir": "",
        "output_filename": "",
        "n_books": 4,
        "book_one_drivers_file": "",
        "date": "",
    }


# Food placeId.
DEPOT_PLACE_ID: Final[str] = "ChIJFw9CDZejhVQRizqiyJSmPqo"


class ExtraNotes:
    """Extra notes for the combined routes.

    Is a class so it appears in docs.
    """

    notes: Final[list[tuple[str, str]]] = [
        # ("Cascade Meadows Apartments*", ""),
        # ("Deer Run Terrace Apartments*", ""),
        # ("Eleanor Apartments*", ""),
        # ("Evergreen Ridge Apartments*", ""),
        # ("Gardenview Village*", ""),
        # ("Heart House*", ""),
        # ("Laurel Forest Apartments*", ""),
        # ("Laurel Village*", ""),
        # ("Park Ridge Apartments*", ""),
        # ("Regency Park Apartments*", ""),
        # ("Sterling Senior Apartments*", ""),
        # ("Trailview Apartments*", ""),
        # ("Tullwood Apartments*", ""),
        # ("Varsity Village*", ""),
        # ("Walton Place*", ""),
        # ("Washington Square Apartments*", ""),
        # ("Woodrose Apartments*", ""),
        # ("Washington Grocery Building*", ""),
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


class IntermediateColumns:
    """Column name constants for intermediate tables."""

    DRIVER_SHEET_NAME: Final[str] = "driver_sheet_name"
    ROUTE_TITLE: Final[str] = "route_title"


MANIFEST_DATE_FORMAT: Final[str] = "%m.%d"

MAX_ORDER_COUNT: Final[int] = 5

NOTES_COLUMN_WIDTH: Final[float] = 56.67

PROTEIN_BOX_TYPES: Final[list[str]] = ["BASIC", "GF", "LA"]


class RateLimits:
    """Rate limits for Circuit API."""

    BATCH_STOP_IMPORT_SECONDS: Final[float] = 1 / (10 / 60)
    BATCH_STOP_IMPORT_MAX_STOPS: Final[int] = 1000
    OPTIMIZATION_PER_SECOND: Final[float] = 1 / (3 / 60)
    READ_TIMEOUT_SECONDS: Final[int] = 10
    READ_SECONDS: Final[float] = 1 / 10
    WRITE_SECONDS: Final[float] = 1 / 5


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
