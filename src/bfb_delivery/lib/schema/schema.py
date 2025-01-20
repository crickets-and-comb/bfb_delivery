"""The data schema for ETL steps."""

from typing import Any

import pandera as pa
from pandera.typing import Series

from bfb_delivery.lib.constants import DEPOT_PLACE_ID, BoxType, Columns
from bfb_delivery.lib.schema import checks  # noqa: F401


class CircuitPlansOut(pa.DataFrameModel):
    """The schema for the Circuit plans data.

    bfb_delivery.lib.dispatch.read_circuit._make_plans_df output.
    """

    # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    id: Series[str] = pa.Field(coerce=True, unique=True, str_startswith="plans/")
    # e.g. "1.17 Andy W":
    title: Series[str] = pa.Field(coerce=True, unique=True)


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

    # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    plan: Series[str] = pa.Field(coerce=True, str_startswith="plans/")
    # route id e.g. "routes/lITTnQsxYffqJQDxIpzr", but within dict at this step.
    # Not hashable, so no unique check.
    route: Series[dict[str, Any]] = pa.Field(coerce=True)
    # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
    id: Series[str] = pa.Field(
        coerce=True, unique=True, str_startswith="plans/", str_contains="/stops/"
    )
    stopPosition: Series[int] = pa.Field(coerce=True, ge=0)
    recipient: Series[dict[str, Any]] = pa.Field(coerce=True)
    address: Series[dict[str, Any]] = pa.Field(coerce=True)
    notes: Series[str] = pa.Field(coerce=True, nullable=True)
    orderInfo: Series[dict[str, Any]] = pa.Field(coerce=True)
    packageCount: Series[float] = pa.Field(coerce=True, nullable=True, eq=1)
    title: Series[str] = pa.Field(coerce=True)

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


class CircuitRoutesTransformIn(CircuitRoutesConcatOut):
    """The schema for the Circuit routes data before transformation.

    bfb_delivery.lib.dispatch.read_circuit._transform_routes_df input.
    """


# TODO: Do dict field validations on input to tx.
class CircuitRoutesTransformOut(pa.DataFrameModel):
    """The schema for the Circuit routes data after transformation.

    bfb_delivery.lib.dispatch.read_circuit._transform_routes_df output.
    """

    # Main output columns for downstream processing.
    # route id e.g. "routes/lITTnQsxYffqJQDxIpzr".
    route: Series[str] = pa.Field(coerce=True, str_startswith="routes/")
    # Renamed "title" column, e.g. "1.17 Andy W":
    driver_sheet_name: Series[str] = pa.Field(coerce=True, at_least_two_words=True)
    stop_no: Series[int] = pa.Field(coerce=True, ge=1, alias=Columns.STOP_NO)
    name: Series[str] = pa.Field(coerce=True, alias=Columns.NAME)
    # TODO: Find address validator tool.
    address: Series[str] = pa.Field(coerce=True, alias=Columns.ADDRESS)
    phone: Series[str] = pa.Field(coerce=True, nullable=True, alias=Columns.PHONE)
    notes: Series[str] = pa.Field(coerce=True, nullable=True, alias=Columns.NOTES)
    order_count: Series[float] = pa.Field(coerce=True, eq=1, alias=Columns.ORDER_COUNT)
    box_type: Series[pa.Category] = pa.Field(
        coerce=True,
        alias=Columns.BOX_TYPE,
        in_list_case_insensitive={"category_list": BoxType},
    )
    neighborhood: Series[str] = pa.Field(
        coerce=True, nullable=True, alias=Columns.NEIGHBORHOOD
    )
    email: Series[str] | None = pa.Field(coerce=True, nullable=True, alias=Columns.EMAIL)

    # Ancillary columns.
    # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    plan: Series[str] = pa.Field(coerce=True, str_startswith="plans/")
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
        many_to_one = {"many_col": "id", "one_col": "plan"}
        many_to_one = {"many_col": "id", "one_col": "route"}
        many_to_one = {"many_col": "id", "one_col": "driver_sheet_name"}
        at_least_one_in_group_str = {"group_col": "plan", "at_least_one_col": "id"}
        at_least_one_in_group_str = {"group_col": "route", "at_least_one_col": "id"}
        at_least_one_in_group_str = {
            "group_col": "driver_sheet_name",
            "at_least_one_col": "id",
        }

        contiguous_group = {
            "group_col": "driver_sheet_name",
            "contiguous_col": Columns.STOP_NO,
            "start_idx": 1,
        }
        increasing_by = {"cols": ["driver_sheet_name", Columns.STOP_NO]}


class CircuitRoutesWriteIn(CircuitRoutesTransformOut):
    """The schema for the Circuit routes data before writing.

    bfb_delivery.lib.dispatch.read_circuit._write_routes_df input.
    """
