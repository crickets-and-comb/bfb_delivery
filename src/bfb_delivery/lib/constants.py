"""Constants used in the project."""

from typing import Final


class Columns:
    """Column name constants."""

    ADDRESS: Final[str] = "Address"
    BOX_TYPE: Final[str] = "Box Type"
    DRIVER: Final[str] = "Driver"  # TODO: Accept any case of columns?
    EMAIL: Final[str] = "Email"
    NAME: Final[str] = "Name"
    NOTES: Final[str] = "Notes"
    ORDER_COUNT: Final[str] = "Order Count"
    PHONE: Final[str] = "Phone"


SPLIT_ROUTE_COLUMNS: Final[list[str]] = [
    Columns.NAME,
    Columns.ADDRESS,
    Columns.PHONE,
    Columns.EMAIL,
    Columns.NOTES,
    Columns.ORDER_COUNT,
    Columns.BOX_TYPE,
]

# TODO: Make box type enum? (Use Pandera?)
