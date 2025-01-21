"""The data schema for ETL steps."""

from functools import partial
from typing import Any

import pandera as pa
from pandera.typing import Series

from bfb_delivery.lib.constants import DEPOT_PLACE_ID, BoxType, Columns
from bfb_delivery.lib.schema import checks  # noqa: F401

ADDRESS_FIELD: Series[str] = partial(pa.Field, coerce=True, alias=Columns.ADDRESS)
BOX_TYPE_FIELD: Series[pa.Category] = partial(
    pa.Field,
    coerce=True,
    alias=Columns.BOX_TYPE,
    in_list_case_insensitive={"category_list": BoxType},
)
# Renamed "title" column, e.g. "1.17 Andy W":
DRIVER_SHEET_NAME_FIELD: Series[str] = partial(pa.Field, coerce=True, at_least_two_words=True)
EMAIL_FIELD: Any = partial(pa.Field, coerce=True, nullable=True, alias=Columns.EMAIL)
NAME_FIELD: Series[str] = partial(pa.Field, coerce=True, alias=Columns.NAME)
NEIGHBORHOOD_FIELD: Series[str] = partial(
    pa.Field, coerce=True, nullable=True, alias=Columns.NEIGHBORHOOD
)
NOTES_FIELD: Series[str] = partial(pa.Field, coerce=True, nullable=True, alias=Columns.NOTES)
ORDER_COUNT_FIELD: Series[float] = partial(
    pa.Field, coerce=True, eq=1, alias=Columns.ORDER_COUNT
)
PHONE_FIELD: Series[str] = partial(pa.Field, coerce=True, nullable=True, alias=Columns.PHONE)
# plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
PLAN_ID_FIELD: Series[str] = partial(pa.Field, coerce=True, str_startswith="plans/")
STOP_NO_FIELD: Series[int] = partial(pa.Field, coerce=True, ge=1, alias=Columns.STOP_NO)


class CircuitPlansOut(pa.DataFrameModel):
    """The schema for the Circuit plans data.

    bfb_delivery.lib.dispatch.read_circuit._make_plans_df output.
    """

    # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    id: Series[str] = pa.Field(coerce=True, unique=True, str_startswith="plans/")
    # e.g. "1.17 Andy W":
    title: Series[str] = DRIVER_SHEET_NAME_FIELD()


class CircuitPlansFromDict(CircuitPlansOut):
    """The schema for the Circuit plans data from a dict.

    bfb_delivery.lib.dispatch.read_circuit._make_plans_df input.
    """

    class Config:
        """The configuration for the schema."""

        from_format = "dict"


class CircuitRoutesConcatInPlans(CircuitPlansOut):
    """The schema for the Circuit plans data.

    bfb_delivery.lib.dispatch.read_circuit._concat_routes_df input.
    """


# TODO: Validate that All HHs driver dropped.
class CircuitRoutesConcatOut(pa.DataFrameModel):
    """The schema for the Circuit routes data.

    bfb_delivery.lib.dispatch.read_circuit._concat_routes_df output.
    """

    plan: Series[str] = PLAN_ID_FIELD()
    route: Series[dict[str, Any]] = pa.Field(coerce=True, item_in_field_dict="id")
    # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
    id: Series[str] = pa.Field(
        coerce=True, unique=True, str_startswith="plans/", str_contains="/stops/"
    )
    # Position 0 is depot, which gets dropped later for the manifests.
    stopPosition: Series[int] = pa.Field(coerce=True, ge=0)
    recipient: Series[dict[str, Any]] = pa.Field(coerce=True, item_in_field_dict="name")
    address: Series[dict[str, Any]] = pa.Field(coerce=True, item_in_field_dict="placeId")
    notes: Series[str] = pa.Field(coerce=True, nullable=True)
    orderInfo: Series[dict[str, Any]] = pa.Field(coerce=True, item_in_field_dict="products")
    packageCount: Series[float] = pa.Field(coerce=True, nullable=True, eq=1)
    title: Series[str] = DRIVER_SHEET_NAME_FIELD()

    class Config:
        """The configuration for the schema."""

        one_to_one = {"col_a": "plan", "col_b": "title"}
        many_to_one = {"many_col": "id", "one_col": "plan"}
        unique_group = {"group_col": "plan", "unique_col": "stopPosition"}
        contiguous_group = {
            "group_col": "plan",
            "contiguous_col": "stopPosition",
            "start_idx": 0,
        }
        item_in_dict_col = {"col_name": "address", "item_name": "placeId"}
        item_in_dict_col = {"col_name": "address", "item_name": "addressLineOne"}
        item_in_dict_col = {"col_name": "address", "item_name": "addressLineTwo"}


class CircuitRoutesTransformIn(CircuitRoutesConcatOut):
    """The schema for the Circuit routes data before transformation.

    bfb_delivery.lib.dispatch.read_circuit._transform_routes_df input.
    """


class CircuitRoutesTransformOut(pa.DataFrameModel):
    """The schema for the Circuit routes data after transformation.

    bfb_delivery.lib.dispatch.read_circuit._transform_routes_df output.
    """

    # Main output columns for downstream processing.
    # route id e.g. "routes/lITTnQsxYffqJQDxIpzr".
    route: Series[str] = pa.Field(coerce=True, str_startswith="routes/")
    driver_sheet_name: Series[str] = DRIVER_SHEET_NAME_FIELD()
    stop_no: Series[int] = STOP_NO_FIELD()
    name: Series[str] = NAME_FIELD()
    # TODO: Find address validator tool.
    address: Series[str] = ADDRESS_FIELD()
    phone: Series[str] = PHONE_FIELD()
    notes: Series[str] = NOTES_FIELD()
    order_count: Series[float] = ORDER_COUNT_FIELD()
    box_type: Series[pa.Category] = BOX_TYPE_FIELD()
    neighborhood: Series[str] = NEIGHBORHOOD_FIELD()
    email: Series[str] = EMAIL_FIELD()

    # Ancillary columns.
    plan: Series[str] = PLAN_ID_FIELD()
    # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
    id: Series[str] = pa.Field(
        coerce=True, unique=True, str_startswith="plans/", str_contains="/stops/"
    )
    placeId: Series[str] = pa.Field(coerce=True, ne=DEPOT_PLACE_ID)

    class Config:
        """The configuration for the schema."""

        # These are redundant in combination, and the "true" relationships aren't explicitly
        # defined as circularly in Circuit, but the data set is small, so not a real cost to
        # be this clear and robust.
        # Also, the Circuit plan:route relationship is 1:m, but we only ever want 1:1.
        unique = ["plan", Columns.STOP_NO]
        unique = ["route", Columns.STOP_NO]
        unique = ["driver_sheet_name", Columns.STOP_NO]
        one_to_one = {"col_a": "route", "col_b": "plan"}
        one_to_one = {"col_a": "route", "col_b": "driver_sheet_name"}
        one_to_one = {"col_a": "plan", "col_b": "driver_sheet_name"}
        at_least_one_in_group = {"group_col": "plan", "at_least_one_col": "driver_sheet_name"}
        at_least_one_in_group = {"group_col": "driver_sheet_name", "at_least_one_col": "plan"}
        at_least_one_in_group = {
            "group_col": "route",
            "at_least_one_col": "driver_sheet_name",
        }
        at_least_one_in_group = {
            "group_col": "driver_sheet_name",
            "at_least_one_col": "route",
        }
        at_least_one_in_group = {"group_col": "route", "at_least_one_col": "plan"}
        at_least_one_in_group = {"group_col": "plan", "at_least_one_col": "route"}
        many_to_one = {"many_col": "id", "one_col": "plan"}
        many_to_one = {"many_col": "id", "one_col": "route"}
        many_to_one = {"many_col": "id", "one_col": "driver_sheet_name"}
        at_least_one_in_group = {"group_col": "plan", "at_least_one_col": "id"}
        at_least_one_in_group = {"group_col": "route", "at_least_one_col": "id"}
        at_least_one_in_group = {"group_col": "driver_sheet_name", "at_least_one_col": "id"}
        at_least_one_in_group = {"group_col": "plan", "at_least_one_col": Columns.STOP_NO}
        at_least_one_in_group = {"group_col": "route", "at_least_one_col": Columns.STOP_NO}
        at_least_one_in_group = {
            "group_col": "driver_sheet_name",
            "at_least_one_col": Columns.STOP_NO,
        }

        contiguous_group = {
            "group_col": "driver_sheet_name",
            "contiguous_col": Columns.STOP_NO,
            "start_idx": 1,
        }
        increasing_by = {"cols": ["driver_sheet_name", Columns.STOP_NO]}


class CircuitRoutesWriteIn(pa.DataFrameModel):
    """The schema for the Circuit routes data before writing.

    bfb_delivery.lib.dispatch.read_circuit._write_routes_df input.
    """

    driver_sheet_name: Series[str] = DRIVER_SHEET_NAME_FIELD()
    stop_no: Series[int] = STOP_NO_FIELD()
    name: Series[str] = NAME_FIELD()
    # TODO: Find address validator tool.
    address: Series[str] = ADDRESS_FIELD()
    phone: Series[str] = PHONE_FIELD()
    notes: Series[str] = NOTES_FIELD()
    order_count: Series[float] = ORDER_COUNT_FIELD()
    box_type: Series[pa.Category] = BOX_TYPE_FIELD()
    neighborhood: Series[str] = NEIGHBORHOOD_FIELD()
    email: Series[str] | None = EMAIL_FIELD()

    class Config:
        """The configuration for the schema."""

        one_to_one = {"col_a": "route", "col_b": "driver_sheet_name"}
        at_least_one_in_group = {
            "group_col": "route",
            "at_least_one_col": "driver_sheet_name",
        }
        at_least_one_in_group = {
            "group_col": "driver_sheet_name",
            "at_least_one_col": "route",
        }
        unique = ["driver_sheet_name", Columns.STOP_NO]
        # TODO: Need to write float/int version.
        at_least_one_in_group = {
            "group_col": "driver_sheet_name",
            "at_least_one_col": Columns.STOP_NO,
        }

        contiguous_group = {
            "group_col": "driver_sheet_name",
            "contiguous_col": Columns.STOP_NO,
            "start_idx": 1,
        }
        increasing_by = {"cols": ["driver_sheet_name", Columns.STOP_NO]}


class CircuitRoutesWriteInAllHHs(CircuitRoutesWriteIn):
    """The schema for the Circuit routes data before writing for "All HHs".

    bfb_delivery.lib.dispatch.read_circuit._write_routes_df_all_hhs input.
    """

    email: Series[str] = EMAIL_FIELD()

    class Config:
        """The configuration for the schema."""

        many_to_one = {"many_col": Columns.STOP_NO, "one_col": "driver_sheet_name"}


class CircuitRoutesWriteOut(pa.DataFrameModel):
    """The schema for the Circuit routes data after writing.

    bfb_delivery.lib.dispatch.read_circuit._write_routes_df input,
    called within _write_routes_df as its "output."
    """

    stop_no: Series[int] = pa.Field(
        coerce=True, unique=True, ge=1, contiguous=1, is_sorted=True, alias=Columns.STOP_NO
    )
    name: Series[str] = NAME_FIELD()
    # TODO: Find address validator tool.
    address: Series[str] = ADDRESS_FIELD()
    phone: Series[str] = PHONE_FIELD()
    notes: Series[str] = NOTES_FIELD()
    box_type: Series[pa.Category] = BOX_TYPE_FIELD()
    neighborhood: Series[str] = NEIGHBORHOOD_FIELD()
    email: Series[str] | None = EMAIL_FIELD()

    class Config:
        """The configuration for the schema."""

        unique = [Columns.NAME, Columns.ADDRESS, Columns.BOX_TYPE]


class CircuitRoutesWriteOutAllHHs(CircuitRoutesWriteOut):
    """The schema for the Circuit routes data after writing.

    bfb_delivery.lib.dispatch.read_circuit._write_route_df_all_hhs input.
    called within _write_routes_dfs_all_hhs as its "output."
    """

    email: Series[str] = EMAIL_FIELD()
