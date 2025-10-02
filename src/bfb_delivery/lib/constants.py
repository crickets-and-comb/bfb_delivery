"""Constants used in the project."""

from enum import StrEnum
from typing import Final

import pandas as pd

from comb_utils import DocString, ErrorDocString

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


class CellColors:
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

    ACTIVE: Final[str] = "active"
    ADDRESS: Final[str] = "address"
    ADDRESS_LINE_1: Final[str] = "addressLineOne"
    ADDRESS_LINE_2: Final[str] = "addressLineTwo"
    ALLOWED_DRIVERS: Final[str] = "allowedDrivers"
    CANCELED: Final[str] = "canceled"
    CITY: Final[str] = "city"
    CODE: Final[str] = "code"
    COUNTRY: Final[str] = "country"
    DAY: Final[str] = "day"
    DEPOT: Final[str] = "depot"
    DISTRIBUTED: Final[str] = "distributed"
    DONE: Final[str] = "done"
    DRIVERS: Final[str] = "drivers"
    EMAIL: Final[str] = "email"
    EXTERNAL_ID: Final[str] = "externalId"
    ID: Final[str] = "id"
    METADATA: Final[str] = "metadata"
    MONTH: Final[str] = "month"
    NAME: Final[str] = "name"
    NOTES: Final[str] = "notes"
    OPERATIONS: Final[str] = "operations"
    OPTIMIZATION: Final[str] = "optimization"
    ORDER_INFO: Final[str] = "orderInfo"
    PACKAGE_COUNT: Final[str] = "packageCount"
    PHONE: Final[str] = "phone"
    PLACE_ID: Final[str] = "placeId"
    PLAN: Final[str] = "plan"
    PLANS: Final[str] = "plans"
    PRODUCTS: Final[str] = "products"
    RECIPIENT: Final[str] = "recipient"
    RESULT: Final[str] = "result"
    ROUTE: Final[str] = "route"
    SKIPPED_STOPS: Final[str] = "skippedStops"
    STARTS: Final[str] = "starts"
    STATE: Final[str] = "state"
    STOP_POSITION: Final[str] = "stopPosition"
    STOPS: Final[str] = "stops"
    TITLE: Final[str] = "title"
    WRITABLE: Final[str] = "writable"
    YEAR: Final[str] = "year"
    ZIP: Final[str] = "zip"


CIRCUIT_URL: Final[str] = "https://api.getcircuit.com/public/v0.2b"
CIRCUIT_DRIVERS_URL: Final[str] = f"{CIRCUIT_URL}/drivers"


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


CIRCUIT_DATE_FORMAT: Final[str] = "%Y-%m-%d"

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


# TODO: Combine with DocString?
# https://github.com/crickets-and-comb/bfb_delivery/issues/57
class Defaults:
    """Default values. E.g., for syncing public API with CLI."""

    BUILD_ROUTES_FROM_CHUNKED: Final[dict[str, str | bool]] = {
        "output_dir": "",
        "start_date": "",
        "no_distribute": False,
        "verbose": False,
        "book_one_drivers_file": "",
        "extra_notes_file": "",
    }
    COMBINE_ROUTE_TABLES: Final[dict[str, str]] = {"output_dir": "", "output_filename": ""}
    CREATE_MANIFESTS: Final[dict[str, str]] = {
        "output_dir": "",
        "output_filename": "",
        "extra_notes_file": "",
    }
    CREATE_MANIFESTS_FROM_CIRCUIT: Final[dict[str, str | bool | list]] = {
        "start_date": "",
        "end_date": "",
        "plan_ids": [],
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


class DocStrings:
    """Docstrings for the public API."""

    BUILD_ROUTES_FROM_CHUNKED: Final = DocString(
        opening="""
Build and disbribute routes from chunked routes.

From a chunked route spreadsheet, builds, optimizes, and distributes routes to drivers.
Produces a final manifest spreadsheet.

Prompts interactive user input to confirm driver assignments.

See :doc:`build_routes_from_chunked` for more information.
""",
        args={
            "input_path": "Path to the chunked route spreadsheet.",
            "output_dir": (
                "Path to the output directory. Empty defaults to a new directory in the "
                'present working directory, named "deliveries_{date}".'
            ),
            "start_date": (
                'The date to start the routes, as "YYYY-MM-DD". Empty string defaults to '
                "the soonest Friday."
            ),
            "no_distribute": "To skip distributing the routes to drivers after optimizing.",
            "verbose": "Whether to print verbose output.",
            "extra_notes_file": (
                "Path to the extra notes file. If empty, uses a constant DataFrame. "
                "See :py:data:`bfb_delivery.lib.constants.ExtraNotes`."
            ),
        },
        returns=["The path to the final manifest spreadsheet."],
        raises=[],
    )

    COMBINE_ROUTE_TABLES: Final = DocString(
        opening="""
Combines the driver route CSVs into a single workbook.

This is used after optimizing and exporting the routes to individual CSVs. It prepares the
worksheets to be formatted with :py:func:`bfb_delivery.api.public.format_combined_routes`.

If `output_dir` is specified, will create the directory if it doesn't exist.

.. note::

    Changes "Product Type" column name back to "Box Type".

See :doc:`combine_route_tables` for more information.
""",
        args={
            "input_dir": "The directory containing the driver route CSVs.",
            "output_dir": (
                "The directory to write the output workbook to. "
                "Empty string (default) saves to the `input_dir` directory."
            ),
            "output_filename": (
                "The name of the output workbook. "
                "Empty string (default) will name the file 'combined_routes_{date}.xlsx'."
            ),
        },
        raises=[
            ErrorDocString(error_type="ValueError", docstring="If `input_paths` is empty.")
        ],
        returns=["The path to the output workbook."],
    )

    CREATE_MANIFESTS: Final = DocString(
        opening="""
From Circuit route CSVs, creates driver manifest workbook ready to print.

This is used after optimizing and exporting the routes to individual CSVs. Reads in
driver route CSVs from `input_dir` and creates a formatted workbook with driver
manifests ready to print, with headers, aggregate data, and color-coded box types. Each
driver's route is a separate sheet in the workbook.

The workbook is saved to `output_dir` with the name `output_filename`. Will create
`output_dir` if it doesn't exist.

.. note::

    Uses the date of the front of each CSV name to set the manifest date field. I.e.,
    each sheet should be named something like "08.08 Richard N", and, e.g., this would
    set the manifest date field to "Date: 08.08".

Just wraps :py:func:`bfb_delivery.api.public.combine_route_tables` and
:py:func:`bfb_delivery.api.public.format_combined_routes`. Creates an intermediate output
workbook with all routes combined, then formats it.

See :doc:`create_manifests` for more information.
""",
        args={
            "input_dir": "The directory containing the driver route CSVs.",
            "output_dir": (
                "The directory to write the formatted manifest workbook to. "
                "Empty string (default) saves to the `input_dir` directory."
            ),
            "output_filename": (
                "The name of the output workbook."
                'Empty string sets filename to "final_manifests_{date}.xlsx".'
            ),
            "extra_notes_file": (
                "Path to the extra notes file. If empty (default), uses a constant "
                "DataFrame. See :py:data:`bfb_delivery.lib.constants.ExtraNotes`."
            ),
        },
        returns=["Path to the formatted manifest workbook."],
        raises=[],
    )

    CREATE_MANIFESTS_FROM_CIRCUIT: Final = DocString(
        opening="""
Gets optimized routes from Circuit, creates driver manifest workbook ready to print.

This is used after uploading and optimizing the routes. Reads routes CSVs from Circuit,
and creates a formatted workbook with driver manifests ready to print, with headers,
aggregate data, and color-coded box types. Each driver's route is a separate sheet in the
workbook.

The workbook is saved to `output_dir` with the name `output_filename`. Will create
`output_dir` if it doesn't exist.

.. note::

    Uses the date of the front of each CSV name to set the manifest date field. I.e.,
    each sheet should be named something like "08.08 Richard N", and, e.g., this would
    set the manifest date field to "Date: 08.08". **But, this does not determine the
    search date range.**

Wraps :py:func:`bfb_delivery.api.public.create_manifests` and adds Circuit integration.
And, `create_manifests` just wraps :py:func:`bfb_delivery.api.public.combine_route_tables`
and :py:func:`bfb_delivery.api.public.format_combined_routes`. Creates an intermediate
output workbook with all routes combined, then formats it.

See :doc:`create_manifests_from_circuit` for more information.
""",
        args={
            "start_date": (
                'The start date to use in the output workbook sheetnames as "YYYYMMDD". '
                "Empty string uses the soonest Friday. Range is inclusive."
            ),
            "end_date": (
                'The end date to use in the output workbook sheetnames as "YYYYMMDD". '
                "Empty string uses the start date. Range is inclusive."
            ),
            "plan_ids": (
                "The list of plan IDs to filter the Circuit routes by. Overrides `all_hhs`. "
                "Not valid for CLI."
            ),
            "output_dir": (
                "The directory to write the formatted manifest workbook to. "
                "Empty string saves to the `input_dir` directory."
            ),
            "output_filename": (
                "The name of the output workbook. "
                'Empty string sets filename to "final_manifests_{date}.xlsx".'
            ),
            "circuit_output_dir": (
                "The directory to create a subdir to save the routes to. Creates "
                '"routes_{date}" directory within the `circuit_output_dir`. Empty string '
                "uses `output_dir`. If the directory does not exist, it is created. If it "
                "exists, it is overwritten."
            ),
            "all_hhs": (
                'Flag to get only the "All HHs" route. '
                'False gets all routes except "All HHs". True gets only the "All HHs" route. '
                "Overridden by `plan_ids`."
            ),
            "verbose": "Flag to print verbose output.",
            "extra_notes_file": (
                "Path to the extra notes file. If empty, uses a constant "
                "DataFrame. See :py:data:`bfb_delivery.lib.constants.ExtraNotes`."
            ),
        },
        returns=["Path to the final manifest workbook."],
        raises=[],
    )

    FORMAT_COMBINED_ROUTES: Final = DocString(
        opening="""
Formats the combined routes table into driver manifests to print.

Adds headers and aggregate data. Color codes box types.

This is used after combining the driver route CSVs into a single workbook
using :py:func:`bfb_delivery.api.public.combine_route_tables`.

If `output_dir` is specified, will create the directory if it doesn't exist.

.. note::

    Uses the date of the front of each sheet name to set the manifest date field. I.e.,
    each sheet should be named something like "05.27 Oscar W", and, e.g., this would set
    the manifest date field to "Date: 05.27".

See :doc:`format_combined_routes` for more information.
""",
        args={
            "input_path": "The path to the combined routes table.",
            "output_dir": (
                "The directory to write the formatted table to. "
                "Empty string (default) saves to the input path's parent directory."
            ),
            "output_filename": (
                "The name of the formatted workbook. "
                'Empty string (default) will name the file "formatted_routes_{date}.xlsx".'
            ),
            "extra_notes_file": (
                "The path to the extra notes file. If empty (default), uses a "
                "constant DataFrame. See :py:data:`bfb_delivery.lib.constants.ExtraNotes`."
            ),
        },
        returns=["The path to the formatted table."],
        raises=[],
    )

    SPLIT_CHUNKED_ROUTE: Final = DocString(
        opening="""
Split route sheet into n workbooks with sheets by driver.

Sheets by driver allows splitting routes by driver on Circuit upload.
Multiple workbooks allows team to split the uploads among members, so one person
doesn't have to upload all routes.
This process follows the "chunking" process in the route generation, where routes
are split into smaller "chunks" by driver (i.e., each stop is labeled with a driver).

Reads a route spreadsheet at `input_path`.
Writes `n_books` Excel workbooks with each sheet containing the stops for a single driver.
Writes adjacent to the original workbook unless `output_dir` specified. If specified, will
create the directory if it doesn't exist.

.. note::

    Renames "Box Type" column name to "Product Type", per Circuit API.

.. note::

    The date passed sets the date in the sheet names of the output workbooks, and that
    date in the sheet name is used for the manifest date field in later functions that
    make the manifests: :py:func:`bfb_delivery.api.public.format_combined_routes` and
    :py:func:`bfb_delivery.api.public.create_manifests_from_circuit` (which wraps the former).


See :doc:`split_chunked_route` for more information.
""",
        args={
            "input_path": (
                "Path to the chunked route sheet that this function reads in and "
                "splits up."
            ),
            "output_dir": (
                "Directory to save the output workbook. "
                "Empty string saves to the input `input_path` directory."
            ),
            "output_filename": (
                "Name of the output workbook. "
                'Empty string sets filename to "split_workbook_{date}_{i of n_books}.xlsx".'
            ),
            "n_books": "Number of workbooks to split into.",
            # TODO: Standardize some of these definitions as well.
            # https://github.com/crickets-and-comb/bfb_delivery/issues/58
            "book_one_drivers_file": (
                "Path to the book-one driver's file. If empty, uses a constant list. "
                "See :py:data:`bfb_delivery.lib.constants.BookOneDrivers`."
            ),
            "date": (
                "The date to use in the output workbook sheetnames. Empty string (default) "
                "uses the soonest Friday."
            ),
        },
        returns=["Paths to the split chunked route workbooks."],
        raises=[
            ErrorDocString(error_type="ValueError", docstring="If `n_books` is less than 1."),
            ErrorDocString(
                error_type="ValueError",
                docstring=(
                    "If `n_books` is greater than the number of drivers in the input "
                    "workbook."
                ),
            ),
        ],
    )


# Food placeId.
DEPOT_PLACE_ID: Final[str] = "ChIJFw9CDZejhVQRizqiyJSmPqo"


class ExtraNotes:
    """Extra notes to add at bottom of manifests when any stop is tagged.

    Is a class so it appears in docs.
    """

    #: Tuples of tag and note. Example: ("Cascade Meadows Apartments*", "Note here.")
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

    #: Extra notes DataFrame. The columns are "tag" and "note". Made from `notes`.
    df: Final[pd.DataFrame]

    def __init__(self) -> None:
        """Initialize the extra notes `df`."""
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

    DRIVER_NAME: Final[str] = "driver_name"
    DRIVER_SHEET_NAME: Final[str] = "driver_sheet_name"
    INITIALIZED: Final[str] = "initialized"
    OPTIMIZED: Final[str] = "optimized"
    PLAN_ID: Final[str] = "plan_id"
    ROUTE_TITLE: Final[str] = "route_title"  # AKA plan title, since 1:1.
    SHEET_NAME: Final[str] = "sheet_name"
    START_DATE: Final[str] = "start_date"
    STOPS_UPLOADED: Final[str] = "stops_uploaded"


MANIFEST_DATE_FORMAT: Final[str] = "%m.%d"

MAX_ORDER_COUNT: Final[int] = 5

NOTES_COLUMN_WIDTH: Final[float] = 56.67

PROTEIN_BOX_TYPES: Final[list[str]] = ["BASIC", "GF", "LA"]


class RateLimits:
    """Rate limits for Circuit API."""

    BATCH_STOP_IMPORT_SECONDS: Final[float] = 6
    BATCH_STOP_IMPORT_MAX_STOPS: Final[int] = 1000
    OPTIMIZATION_PER_SECOND: Final[float] = 20
    OPTIMIZATION_TIMEOUT_SECONDS: Final[float] = 10
    READ_TIMEOUT_SECONDS: Final[float] = OPTIMIZATION_TIMEOUT_SECONDS
    READ_SECONDS: Final[float] = 0.1
    WAIT_DECREASE_SECONDS: Final[float] = 0.6
    WAIT_INCREASE_SCALAR: Final[float] = 2
    WRITE_SECONDS: Final[float] = 0.2
    WRITE_TIMEOUT_SECONDS: Final[float] = OPTIMIZATION_TIMEOUT_SECONDS


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
