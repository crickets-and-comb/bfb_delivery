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

_COERCE_FIELD = partial(pa.Field, coerce=True)
_NULLABLE_FIELD = partial(_COERCE_FIELD, nullable=True)
_UNIQUE_FIELD = partial(_COERCE_FIELD, unique=True)

ADDRESS_FIELD = partial(_COERCE_FIELD, alias=Columns.ADDRESS)
BOX_TYPE_FIELD = partial(
    _COERCE_FIELD, alias=Columns.BOX_TYPE, in_list_case_insensitive={"category_list": BoxType}
)
# Renamed CircuitColumns.TITLE column, e.g. "1.17 Andy W":
TITLE_FIELD = partial(_COERCE_FIELD, at_least_two_words=True)
EMAIL_FIELD = partial(_NULLABLE_FIELD, alias=Columns.EMAIL)
NAME_FIELD = partial(_COERCE_FIELD, alias=Columns.NAME)
NEIGHBORHOOD_FIELD = partial(_NULLABLE_FIELD, alias=Columns.NEIGHBORHOOD)
NOTES_FIELD = partial(_NULLABLE_FIELD, alias=Columns.NOTES)
ORDER_COUNT_FIELD = partial(_COERCE_FIELD, eq=1, alias=Columns.ORDER_COUNT)
ORDER_INFO_FIELD = partial(
    _COERCE_FIELD, item_in_field_dict=CircuitColumns.PRODUCTS, alias=CircuitColumns.ORDER_INFO
)
PHONE_FIELD = partial(_NULLABLE_FIELD, alias=Columns.PHONE)
# plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
PLAN_ID_FIELD = partial(_COERCE_FIELD, str_startswith="plans/")
ROUTE_FIELD = partial(_COERCE_FIELD, alias=CircuitColumns.ROUTE)
# stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
STOP_ID_FIELD = partial(
    _UNIQUE_FIELD, str_startswith="plans/", str_contains="/stops/", alias=CircuitColumns.ID
)
STOP_NO_FIELD = partial(_COERCE_FIELD, ge=1, alias=Columns.STOP_NO)


class CircuitPlansOut(pa.DataFrameModel):
    """The schema for the Circuit plans data.

    bfb_delivery.lib.dispatch.read_circuit._make_plans_df output.
    """

    # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    id: Series[str] = PLAN_ID_FIELD(unique=True, alias=CircuitColumns.ID)
    # e.g. "1.17 Andy W":
    title: Series[str] = TITLE_FIELD(alias=CircuitColumns.TITLE)

    class Config:
        """The configuration for the schema."""

        strict = True


class CircuitPlansFromDict(CircuitPlansOut):
    """The schema for the Circuit plans data from a JSON-esque dict.

    bfb_delivery.lib.dispatch.read_circuit._make_plans_df input.
    """

    routes: Series[list[str]] = _COERCE_FIELD(is_list_of_one_or_less=True)

    class Config:
        """The configuration for the schema."""

        from_format = "dict"
        strict = False


class CircuitPlansTransformIn(CircuitPlansOut):
    """The schema for the Circuit plans data.

    bfb_delivery.lib.dispatch.read_circuit._transform_routes_df input.
    """


class CircuitRoutesTransformInFromDict(pa.DataFrameModel):
    """The schema for the Circuit routes data from a JSON-esque dict.

    bfb_delivery.lib.dispatch.read_circuit._transform_routes_df input.
    """

    plan: Series[str] = PLAN_ID_FIELD(alias=CircuitColumns.PLAN)
    route: Series[dict[str, Any]] = ROUTE_FIELD(item_in_field_dict=CircuitColumns.ID)
    id: Series[str] = STOP_ID_FIELD()
    # Position 0 is depot, which gets dropped later for the manifests.
    stopPosition: Series[int] = STOP_NO_FIELD(ge=0, alias=CircuitColumns.STOP_POSITION)
    recipient: Series[dict[str, Any]] = _COERCE_FIELD(
        item_in_field_dict=CircuitColumns.NAME, alias=CircuitColumns.RECIPIENT
    )
    address: Series[dict[str, Any]] = ADDRESS_FIELD(
        alias=CircuitColumns.ADDRESS, item_in_field_dict=CircuitColumns.PLACE_ID
    )
    notes: Series[str] = NOTES_FIELD(alias=CircuitColumns.NOTES)
    orderInfo: Series[dict[str, Any]] = ORDER_INFO_FIELD()
    packageCount: Series[float] = _NULLABLE_FIELD(eq=1, alias=CircuitColumns.PACKAGE_COUNT)

    class Config:
        """The configuration for the schema."""

        from_format = "dict"

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


class CircuitRoutesTransformOut(pa.DataFrameModel):
    """The schema for the Circuit routes data after transformation.

    bfb_delivery.lib.dispatch.read_circuit._transform_routes_df output.
    """

    # Main output columns for downstream processing.
    # route id e.g. "routes/lITTnQsxYffqJQDxIpzr".
    route: Series[str] = ROUTE_FIELD(str_startswith="routes/")
    driver_sheet_name: Series[str] = TITLE_FIELD(alias=IntermediateColumns.DRIVER_SHEET_NAME)
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
    plan: Series[str] = PLAN_ID_FIELD(alias=CircuitColumns.PLAN)
    id: Series[str] = STOP_ID_FIELD()
    orderInfo: Series[dict[str, Any]] = ORDER_INFO_FIELD(one_product=True)
    route_title: Series[str] = TITLE_FIELD(alias=IntermediateColumns.ROUTE_TITLE)
    placeId: Series[str] = _COERCE_FIELD(ne=DEPOT_PLACE_ID, alias=CircuitColumns.PLACE_ID)

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
        # TODO: Was violated on 10/4. Investigate, but ignore for now.
        # plans/jEvjLs3ViQkKPBcJVduF, routes/z9AmJkUnuQXUGHGsoxyG
        # Had route title "10.11 Sara" and driver sheet name (plan title) "10.4 Sara"
        # equal_cols = {
        #     "col_a": IntermediateColumns.ROUTE_TITLE,
        #     "col_b": IntermediateColumns.DRIVER_SHEET_NAME,
        # }

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

    driver_sheet_name: Series[str] = TITLE_FIELD(alias=IntermediateColumns.DRIVER_SHEET_NAME)
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
    order_count: Series[float] = ORDER_COUNT_FIELD()
    box_type: Series[pa.Category] = BOX_TYPE_FIELD()
    neighborhood: Series[str] = NEIGHBORHOOD_FIELD()
    email: Series[str] = EMAIL_FIELD()

    class Config:
        """The configuration for the schema."""

        unique = [Columns.NAME, Columns.ADDRESS, Columns.BOX_TYPE]
