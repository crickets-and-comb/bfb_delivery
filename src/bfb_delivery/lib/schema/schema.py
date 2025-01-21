"""The data schema for ETL steps."""

from functools import partial
from typing import Any

import pandera as pa
from pandera.typing import Series

from bfb_delivery.lib.constants import (
    DEPOT_PLACE_ID,
    BoxType,
    CircuitColumns,
    Columns,
    IntermediateColumns,
)
from bfb_delivery.lib.schema import checks  # noqa: F401

ADDRESS_FIELD: Series[str] = partial(pa.Field, coerce=True, alias=Columns.ADDRESS)
BOX_TYPE_FIELD: Series[pa.Category] = partial(
    pa.Field,
    coerce=True,
    alias=Columns.BOX_TYPE,
    in_list_case_insensitive={"category_list": BoxType},
)
# Renamed CircuitColumns.TITLE column, e.g. "1.17 Andy W":
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


class CircuitRoutesConcatOut(pa.DataFrameModel):
    """The schema for the Circuit routes data.

    bfb_delivery.lib.dispatch.read_circuit._concat_routes_df output.
    """

    # TODO: Validate single box count and single type.
    # TODO: Alias the constant columns.

    plan: Series[str] = PLAN_ID_FIELD()
    route: Series[dict[str, Any]] = pa.Field(
        coerce=True, item_in_field_dict=CircuitColumns.ID
    )
    # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
    id: Series[str] = pa.Field(
        coerce=True, unique=True, str_startswith="plans/", str_contains="/stops/"
    )
    # Position 0 is depot, which gets dropped later for the manifests.
    stopPosition: Series[int] = pa.Field(coerce=True, ge=0)
    recipient: Series[dict[str, Any]] = pa.Field(
        coerce=True, item_in_field_dict=CircuitColumns.NAME
    )
    address: Series[dict[str, Any]] = pa.Field(
        coerce=True, item_in_field_dict=CircuitColumns.PLACE_ID
    )
    notes: Series[str] = pa.Field(coerce=True, nullable=True)
    orderInfo: Series[dict[str, Any]] = pa.Field(
        coerce=True, item_in_field_dict=CircuitColumns.PRODUCTS
    )
    packageCount: Series[float] = pa.Field(coerce=True, nullable=True, eq=1)

    class Config:
        """The configuration for the schema."""

        strict = True

        many_to_one = {"many_col": CircuitColumns.ID, "one_col": CircuitColumns.PLAN}
        unique_group = {
            "group_col": CircuitColumns.PLAN,
            "unique_col": CircuitColumns.STOP_POSITION,
        }
        contiguous_group = {
            "group_col": CircuitColumns.PLAN,
            "contiguous_col": CircuitColumns.STOP_POSITION,
            "start_idx": 0,
        }
        item_in_dict_col = {
            "col_name": CircuitColumns.ADDRESS,
            "item_name": CircuitColumns.PLACE_ID,
        }
        item_in_dict_col = {
            "col_name": CircuitColumns.ADDRESS,
            "item_name": CircuitColumns.ADDRESS_LINE_1,
        }
        item_in_dict_col = {
            "col_name": CircuitColumns.ADDRESS,
            "item_name": CircuitColumns.ADDRESS_LINE_2,
        }


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
    route_title: Series[str] = DRIVER_SHEET_NAME_FIELD()
    placeId: Series[str] = pa.Field(coerce=True, ne=DEPOT_PLACE_ID)

    class Config:
        """The configuration for the schema."""

        # These are redundant in combination, and the "true" relationships aren't explicitly
        # defined as circularly in Circuit, but the data set is small, so not a real cost to
        # be this clear and robust.
        # Also, the Circuit plan:route relationship is 1:m, but we only ever want 1:1.
        unique = [CircuitColumns.PLAN, Columns.STOP_NO]
        unique = [CircuitColumns.ROUTE, Columns.STOP_NO]
        unique = [IntermediateColumns.DRIVER_SHEET_NAME, Columns.STOP_NO]
        one_to_one = {"col_a": CircuitColumns.ROUTE, "col_b": CircuitColumns.PLAN}
        one_to_one = {
            "col_a": CircuitColumns.ROUTE,
            "col_b": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        one_to_one = {
            "col_a": CircuitColumns.PLAN,
            "col_b": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.PLAN,
            "at_least_one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "at_least_one_col": CircuitColumns.PLAN,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.ROUTE,
            "at_least_one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "at_least_one_col": CircuitColumns.ROUTE,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.ROUTE,
            "at_least_one_col": CircuitColumns.PLAN,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.PLAN,
            "at_least_one_col": CircuitColumns.ROUTE,
        }
        equal_cols = {
            "col_a": IntermediateColumns.ROUTE_TITLE,
            "col_b": IntermediateColumns.DRIVER_SHEET_NAME,
        }

        many_to_one = {"many_col": CircuitColumns.ID, "one_col": CircuitColumns.PLAN}
        many_to_one = {"many_col": CircuitColumns.ID, "one_col": CircuitColumns.ROUTE}
        many_to_one = {
            "many_col": CircuitColumns.ID,
            "one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.PLAN,
            "at_least_one_col": CircuitColumns.ID,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.ROUTE,
            "at_least_one_col": CircuitColumns.ID,
        }
        at_least_one_in_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "at_least_one_col": CircuitColumns.ID,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.PLAN,
            "at_least_one_col": Columns.STOP_NO,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.ROUTE,
            "at_least_one_col": Columns.STOP_NO,
        }
        at_least_one_in_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "at_least_one_col": Columns.STOP_NO,
        }

        contiguous_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "contiguous_col": Columns.STOP_NO,
            "start_idx": 1,
        }
        increasing_by = {"cols": [IntermediateColumns.DRIVER_SHEET_NAME, Columns.STOP_NO]}


class CircuitRoutesWriteIn(pa.DataFrameModel):
    """The schema for the Circuit routes data before writing.

    bfb_delivery.lib.dispatch.read_circuit._write_routes_df input.
    """

    driver_sheet_name: Series[str] = DRIVER_SHEET_NAME_FIELD()
    stop_no: Series[int] = STOP_NO_FIELD()
    name: Series[str] = NAME_FIELD()
    address: Series[str] = ADDRESS_FIELD()
    phone: Series[str] = PHONE_FIELD()
    notes: Series[str] = NOTES_FIELD()
    order_count: Series[float] = ORDER_COUNT_FIELD()
    box_type: Series[pa.Category] = BOX_TYPE_FIELD()
    neighborhood: Series[str] = NEIGHBORHOOD_FIELD()
    email: Series[str] | None = EMAIL_FIELD()

    class Config:
        """The configuration for the schema."""

        one_to_one = {
            "col_a": CircuitColumns.ROUTE,
            "col_b": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group = {
            "group_col": CircuitColumns.ROUTE,
            "at_least_one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "at_least_one_col": CircuitColumns.ROUTE,
        }
        unique = [IntermediateColumns.DRIVER_SHEET_NAME, Columns.STOP_NO]
        at_least_one_in_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "at_least_one_col": Columns.STOP_NO,
        }

        contiguous_group = {
            "group_col": IntermediateColumns.DRIVER_SHEET_NAME,
            "contiguous_col": Columns.STOP_NO,
            "start_idx": 1,
        }
        increasing_by = {"cols": [IntermediateColumns.DRIVER_SHEET_NAME, Columns.STOP_NO]}


class CircuitRoutesWriteInAllHHs(CircuitRoutesWriteIn):
    """The schema for the Circuit routes data before writing for "All HHs".

    bfb_delivery.lib.dispatch.read_circuit._write_routes_df_all_hhs input.
    """

    email: Series[str] = EMAIL_FIELD()

    class Config:
        """The configuration for the schema."""

        many_to_one = {
            "many_col": Columns.STOP_NO,
            "one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }


class CircuitRoutesWriteOut(pa.DataFrameModel):
    """The schema for the Circuit routes data after writing.

    bfb_delivery.lib.dispatch.read_circuit._write_routes_df input,
    called within _write_routes_df as its "output."
    """

    stop_no: Series[int] = pa.Field(
        coerce=True, unique=True, ge=1, contiguous=1, is_sorted=True, alias=Columns.STOP_NO
    )
    name: Series[str] = NAME_FIELD()
    address: Series[str] = ADDRESS_FIELD()
    phone: Series[str] = PHONE_FIELD()
    notes: Series[str] = NOTES_FIELD()
    box_type: Series[pa.Category] = BOX_TYPE_FIELD()
    neighborhood: Series[str] = NEIGHBORHOOD_FIELD()
    email: Series[str] | None = EMAIL_FIELD()

    class Config:
        """The configuration for the schema."""

        unique = [Columns.NAME, Columns.ADDRESS, Columns.BOX_TYPE]


# TODO: This is unnecessary. Just make it one.
class CircuitRoutesWriteOutAllHHs(CircuitRoutesWriteOut):
    """The schema for the Circuit routes data after writing.

    bfb_delivery.lib.dispatch.read_circuit._write_route_df_all_hhs input.
    called within _write_routes_dfs_all_hhs as its "output."
    """

    email: Series[str] = EMAIL_FIELD()
