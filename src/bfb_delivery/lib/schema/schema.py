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
    _COERCE_FIELD, in_list_case_insensitive={"category_list": BoxType}, alias=Columns.BOX_TYPE
)
DRIVER_ID_FIELD = partial(_COERCE_FIELD, str_startswith="drivers/")
# Renamed CircuitColumns.TITLE column, e.g. "1.17 Andy W":
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
TITLE_FIELD = partial(_COERCE_FIELD, at_least_two_words=True)

# TODO: Unsmurf.
# https://github.com/crickets-and-comb/bfb_delivery/issues/83


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
        item_in_field_dict=CircuitColumns.PLACE_ID, alias=CircuitColumns.ADDRESS
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
        # TODO: Convert back to field checks now that we know they must be uniquely defined
        # either way.
        # https://github.com/crickets-and-comb/bfb_delivery/issues/84
        address1_in_address = True
        address2_in_address = True


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

        # NOTE, the Circuit plan:route relationship is 1:m, but we only ever want 1:1.
        one_to_one = {"col_a": CircuitColumns.ROUTE, "col_b": CircuitColumns.PLAN}
        one_to_one_route_sheet = True
        at_least_one_in_group = {
            "group_col": CircuitColumns.PLAN,
            "at_least_one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group_sheet_plan = True
        at_least_one_in_group_route_sheet = True
        at_least_one_in_group_sheet_route = True

        # TODO: Was violated on 10/4. Investigate, but ignore for now.
        # plans/jEvjLs3ViQkKPBcJVduF, routes/z9AmJkUnuQXUGHGsoxyG
        # Had route title "10.11 Sara" and driver sheet name (plan title) "10.4 Sara"
        # equal_cols = {
        #     "col_a": IntermediateColumns.ROUTE_TITLE,
        #     "col_b": IntermediateColumns.DRIVER_SHEET_NAME,
        # }
        # https://github.com/crickets-and-comb/bfb_delivery/issues/85

        many_to_one = {
            "many_col": CircuitColumns.ID,
            "one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        unique = [IntermediateColumns.DRIVER_SHEET_NAME, Columns.STOP_NO]
        at_least_one_in_group_sheet_stop = True
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
        many_to_one = {
            "many_col": CircuitColumns.ID,
            "one_col": IntermediateColumns.DRIVER_SHEET_NAME,
        }
        at_least_one_in_group_route_sheet = True
        at_least_one_in_group_sheet_route = True

        unique = [IntermediateColumns.DRIVER_SHEET_NAME, Columns.STOP_NO]
        at_least_one_in_group_sheet_stop = True
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


class Stops(pa.DataFrameModel):
    """The schema for the stops data to upload."""

    name: Series[str] = NAME_FIELD()
    address: Series[str] = ADDRESS_FIELD()
    phone: Series[str] = PHONE_FIELD()
    email: Series[str] = EMAIL_FIELD()
    notes: Series[str] = NOTES_FIELD()
    order_count: Series[float] = ORDER_COUNT_FIELD()
    product_type: Series[pa.Category] = BOX_TYPE_FIELD(alias=Columns.PRODUCT_TYPE)
    neighborhood: Series[str] = NEIGHBORHOOD_FIELD()
    sheet_name: Series[str] = TITLE_FIELD(alias=IntermediateColumns.SHEET_NAME)

    class Config:
        """The configuration for the schema."""

        unique = [Columns.NAME, Columns.ADDRESS, Columns.PRODUCT_TYPE]


class DriversGetAllDriversOut(pa.DataFrameModel):
    """The schema for the drivers data after getting all drivers."""

    id: Series[str] = DRIVER_ID_FIELD(unique=True, alias=CircuitColumns.ID)
    name: Series[str] = NAME_FIELD(alias=CircuitColumns.NAME)
    email: Series[str] = EMAIL_FIELD(nullable=False, alias=CircuitColumns.EMAIL)
    active: Series[bool] = _COERCE_FIELD(alias=CircuitColumns.ACTIVE)


class DriversAssignDriversIn(DriversGetAllDriversOut):
    """The schema for the drivers data before assigning drivers."""


class DriversAssignDriverIn(DriversAssignDriversIn):
    """The schema for the driver data before assigning a driver."""


class PlansAssignDriversIn(pa.DataFrameModel):
    """The schema for the plans data before assigning drivers."""

    route_title: Series[str] = TITLE_FIELD(unique=True, alias=IntermediateColumns.ROUTE_TITLE)
    driver_name: Series[str] = NAME_FIELD(
        nullable=True, alias=IntermediateColumns.DRIVER_NAME
    )
    email: Series[str] = EMAIL_FIELD(alias=CircuitColumns.EMAIL)
    id: Series[str] = DRIVER_ID_FIELD(nullable=True, alias=CircuitColumns.ID)


class PlansAssignDriverIn(PlansAssignDriversIn):
    """The schema for the plan data before assigning a driver."""


class PlansAssignDriverOut(PlansAssignDriverIn):
    """The schema for the plan data after assigning a driver."""


class PlansAssignDriversOut(PlansAssignDriverOut):
    """The schema for the plans data after assigning drivers."""


class PlansAssignDriversToPlansOut(PlansAssignDriversOut):
    """The schema for the plans data after assigning drivers."""

    driver_name: Series[str] = NAME_FIELD(alias=IntermediateColumns.DRIVER_NAME)
    email: Series[str] = EMAIL_FIELD(nullable=False, alias=CircuitColumns.EMAIL)
    id: Series[str] = DRIVER_ID_FIELD(alias=CircuitColumns.ID)

    class Config:
        """The configuration for the schema."""

        many_to_one = {
            "many_col": IntermediateColumns.ROUTE_TITLE,
            "one_col": CircuitColumns.ID,  # Driver ID
        }


class PlansInitializePlansIn(PlansAssignDriversToPlansOut):
    """The schema for the plans data before initializing."""


class PlansInitializePlansOut(PlansInitializePlansIn):
    """The schema for the plans data after initializing."""

    plan_id: Series[str] = PLAN_ID_FIELD(
        unique=True, nullable=True, alias=IntermediateColumns.PLAN_ID
    )
    writable: Series[bool] = _NULLABLE_FIELD(alias=CircuitColumns.WRITABLE)
    optimization: Series[str] = _NULLABLE_FIELD(alias=CircuitColumns.OPTIMIZATION)
    initialized: Series[bool] = _COERCE_FIELD(alias=IntermediateColumns.INITIALIZED)

    class Config:
        """The configuration for the schema."""

        # TODO: Make sure this accommodates null plan IDs.
        # https://github.com/crickets-and-comb/bfb_delivery/issues/86
        one_to_one = {
            "col_a": IntermediateColumns.ROUTE_TITLE,
            "col_b": IntermediateColumns.PLAN_ID,
        }


class PlansCreatePlansOut(PlansInitializePlansOut):
    """The schema for the plans data after creating."""


class PlansUploadStopsIn(PlansCreatePlansOut):
    """The schema for the plans data before uploading stops."""


class PlansBuildStopsIn(PlansCreatePlansOut):
    """The schema for the plans data before building stops."""

    # TODO: Refactor to relax the requirements, here and elsewhere.
    # https://github.com/crickets-and-comb/bfb_delivery/issues/87


class PlansUploadStopsOut(PlansUploadStopsIn):
    """The schema for the plan data after uploading stops."""

    stops_uploaded: Series[bool] = _COERCE_FIELD(alias=IntermediateColumns.STOPS_UPLOADED)


# TODO: Build a from/to dict validator for _build_stop_array?
# https://github.com/crickets-and-comb/bfb_delivery/issues/88


class PlansOptimizeRoutesIn(PlansUploadStopsOut):
    """The schema for the plans data before optimizing routes."""


class PlansConfirmOptimizationsIn(PlansOptimizeRoutesIn):
    """The schema for the plans data after before confirming optimizations."""

    routes_optimized: Series[bool] = _COERCE_FIELD(alias=IntermediateColumns.OPTIMIZED)


class PlansConfirmOptimizationsOut(PlansConfirmOptimizationsIn):
    """The schema for the plans data after confirming optimizations."""

    routes_optimized: Series = pa.Field(
        nullable=True, coerce=False, alias=IntermediateColumns.OPTIMIZED
    )


class PlansOptimizeRoutesOut(PlansConfirmOptimizationsOut):
    """The schema for the plans data after optimizing routes."""


class PlansDistributeRoutesIn(PlansOptimizeRoutesOut):
    """The schema for the plans data before distributing routes."""


class PlansDistributeRoutesOut(PlansDistributeRoutesIn):
    """The schema for the plans data after distributing routes."""

    distributed: Series[bool] = _COERCE_FIELD(alias=CircuitColumns.DISTRIBUTED)


class PlansUploadSplitChunkedOut(PlansDistributeRoutesOut):
    """The schema for the plans data after uploading split chunked routes."""
