"""The data schema for ETL steps."""

from typing import Any

import pandera as pa
from pandera.typing import Series

# from bfb_delivery.lib.constants import DEPOT_PLACE_ID, BoxType, Columns
from bfb_delivery.lib.schema import checks  # noqa: F401


class CircuitPlans(pa.DataFrameModel):
    """The schema for the Circuit plans data.

    bfb_delivery.lib.dispatch.read_circuit._make_plans_df output.
    """

    # TODO: How to set field names from column constants? Annotated?
    id: Series[str] = pa.Field(coerce=True, unique=True, str_startswith="plans/")
    title: Series[str] = pa.Field(coerce=True, unique=True)


class CircuitPlansFromDict(CircuitPlans):
    """The schema for the Circuit plans data from a dict.

    bfb_delivery.lib.dispatch.read_circuit._make_plans_df input.
    """

    class Config:
        """The configuration for the schema."""

        from_format = "dict"


class CircuitRoutesConcatInPlans(pa.DataFrameModel):
    """The schema for the Circuit plans data.

    bfb_delivery.lib.dispatch.read_circuit._concat_routes_df input.
    """


class CircuitRoutesConcatOut(pa.DataFrameModel):
    """The schema for the Circuit routes data.

    bfb_delivery.lib.dispatch.read_circuit._concat_routes_df output.
    """

    # TODO: Recycle fields and checks.

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
        check_unique_group = {"group_col": "plan", "unique_col": "stopPosition"}
        check_contiguous_group = {
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

    # # Main output columns for downstream processing.
    # # route id e.g. "routes/lITTnQsxYffqJQDxIpzr".
    # route: Series[str] = pa.Field(coerce=True, str_startswith="routes/")
    # driver_sheet_name: Series[str] = pa.Field(coerce=True)
    # stop_no: Series[int] = pa.Field(coerce=True, ge=1, alias=Columns.STOP_NO)
    # name: Series[str] = pa.Field(coerce=True, alias=Columns.NAME)
    # # TODO: Find address validator tool.
    # address: Series[str] = pa.Field(coerce=True, alias=Columns.ADDRESS)
    # phone: Series[str] = pa.Field(coerce=True, nullable=True, alias=Columns.PHONE)
    # notes: Series[str] = pa.Field(coerce=True, nullable=True, alias=Columns.NOTES)
    # order_count: Series[float] = pa.Field(coerce=True, eq=1, alias=Columns.ORDER_COUNT)
    # box_type: Series[pa.Category] = pa.Field(
    #     coerce=True, alias=Columns.BOX_TYPE, isin=BoxType
    # )
    # neighborhood: Series[str] = pa.Field(
    #     coerce=True, nullable=True, alias=Columns.NEIGHBORHOOD
    # )
    # email: Series[str] = pa.Field(coerce=True, nullable=True, alias=Columns.EMAIL)

    # # Ancillary columns.
    # # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    # plan: Series[str] = pa.Field(coerce=True, str_startswith="plans/")
    # # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
    # id: Series[str] = pa.Field(
    #     coerce=True, unique=True, str_startswith="plans/", str_contains="/stops/"
    # )
    # placeId: Series[str] = pa.Field(coerce=True, ne=DEPOT_PLACE_ID)

    # # Properties.
    # class Config:
    #     """The configuration for the schema."""

    #     # TODO: Does this work?
    #     unique: str | list[str] | None = ["route", Columns.STOP_NO]

    # @pa.check("route", groupby="plan", name="many_to_one_plan_route")
    # def check_many_to_one_plan_route(
    #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    # ) -> bool:
    #     """Check that each plan has only one route."""
    #     return all(route.nunique() == 1 for route in grouped_value.values())

    # @pa.check("plan", groupby="route", name="many_to_one_route_plan")
    # def check_many_to_one_route_plan(
    #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    # ) -> bool:
    #     """Check that each route has only one plan."""
    #     return all(plan.nunique() == 1 for plan in grouped_value.values())

    # @pa.check("route", groupby="id", name="many_to_one_stop_route")
    # def check_many_to_one_stop_route(
    #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    # ) -> bool:
    #     """Check that each stop has only one route."""
    #     return all(route.nunique() == 1 for route in grouped_value.values())

    # @pa.check("driver_sheet_name", name="driver_sheet_name_two_or_more_words")
    # def check_driver_sheet_name_two_or_more_words(
    #     cls, series: Series[str]  # noqa: B902
    # ) -> bool:
    #     """Check that the driver sheet name has two or more words."""
    #     return all(len(sheetName.split()) >= 2 for sheetName in series)

    # @pa.check(Columns.STOP_NO, groupby="route", name="stop_number_unique")
    # def check_stop_position_unique(
    #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    # ) -> bool:
    #     """Check that each route has unique stop numbers."""
    #     return all(
    #         len(stopPosition) == stopPosition.nunique()
    #         for stopPosition in grouped_value.values()
    #     )

    # @pa.check(Columns.STOP_NO, groupby="route", name="stop_number_contiguous")
    # def check_stop_position_contiguous(
    #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    # ) -> bool:
    #     """Check that each route has contiguous stop numbers."""
    #     return all(
    #         sorted(stopPosition.to_list()) == list(range(1, len(stopPosition.to_list()) + 1)) # noqa: E501
    #         for stopPosition in grouped_value.values()
    #     )

    # @pa.check(
    #     "driver_sheet_name", Columns.STOP_NO, name="sort_by_driver_sheet_name_and_stop_no"
    # )
    # def check_sort_by_driver_sheet_name_and_stop_no(
    #     cls, driver_sheet_name: Series[str], stop_no: Series[int]  # noqa: B902
    # ) -> bool:
    #     """Check that the DataFrame is sorted by driver_sheet_name and stop_no."""
    #     return all(
    #         driver_sheet_name[i] < driver_sheet_name[i + 1]
    #         or (  # noqa: W503
    #             driver_sheet_name[i] == driver_sheet_name[i + 1]
    #             and stop_no[i] < stop_no[i + 1]  # noqa: W503
    #         )
    #         for i in range(len(driver_sheet_name) - 1)
    #     )
