"""The data schema for ETL steps."""

from typing import Any

import pandera as pa
from pandera.typing import Series


class CircuitPlans(pa.DataFrameModel):
    """The schema for the Circuit plans data."""

    # TODO: How to set field names from column constants? Annotated?
    id: Series[str] = pa.Field(coerce=True, unique=True, str_startswith="plans/")
    title: Series[str] = pa.Field(coerce=True, unique=True)


class CircuitPlansFromDict(CircuitPlans):
    """The schema for the Circuit plans data from a dict."""

    class Config:
        """The configuration for the schema."""

        from_format = "dict"


class CircuitRoutesConcatOut(pa.DataFrameModel):
    """The schema for the Circuit routes data."""

    # TODO: Recycle fields.

    # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    plan: Series[str] = pa.Field(coerce=True, str_startswith="plans/")
    # route id e.g. "routes/lITTnQsxYffqJQDxIpzr", but within dict at this step.
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

    # TODO: Use these later in the pipeline when IDs have been extracted from dicts.
    # @pa.check("route", groupby="plan", name="many_to_one_plan_route")
    # def check_many_to_one_plan_route(
    #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    # ) -> bool:
    #     """Check that each plan has only one route."""
    #     # NOTE: route is dict and unhashable, so the reciprocal check is not possible.
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

    @pa.check("plan", groupby="id", name="many_to_one_stop_plan")
    def check_many_to_one_stop_plan(
        cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    ) -> bool:
        """Check that each stop has only one plan."""
        return all(plan.nunique() == 1 for plan in grouped_value.values())

    @pa.check("stopPosition", groupby="plan", name="stop_position_unique")
    def check_stop_position_unique(
        cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    ) -> bool:
        """Check that each plan has unique stop positions."""
        return all(
            len(stopPosition) == stopPosition.nunique()
            for stopPosition in grouped_value.values()
        )

    @pa.check("stopPosition", groupby="plan", name="stop_position_continguous")
    def check_stop_position_continguous(
        cls, grouped_value: dict[str, Series[str]]  # noqa: B902
    ) -> bool:
        """Check that each plan has contiguous stop positions."""
        return all(
            sorted(stopPosition.to_list()) == list(range(0, len(stopPosition.to_list())))
            for stopPosition in grouped_value.values()
        )


# # TODO: Do dict field validations on input to tx.
# class CircuitRoutesTransformOut(CircuitRoutesConcatOut):
#     """The schema for the Circuit routes data after transformation."""

#     # route id e.g. "routes/lITTnQsxYffqJQDxIpzr".
#     route: Series[dict[str, Any]] = pa.Field(coerce=True)
#     # stop id e.g. "plans/0IWNayD8NEkvD5fQe2SQ/stops/40lmbcQrd32NOfZiiC1b":
#     id: Series[str] = pa.Field(
#         coerce=True, unique=True, str_startswith="plans/", str_contains="/stops/"
#     )
#     stopPosition: Series[int] = pa.Field(coerce=True, ge=0)
#     recipient: Series[dict[str, Any]] = pa.Field(coerce=True)
#     address: Series[dict[str, Any]] = pa.Field(coerce=True)
#     notes: Series[str] = pa.Field(coerce=True, nullable=True)
#     orderInfo: Series[dict[str, Any]] = pa.Field(coerce=True)
#     packageCount: Series[float] = pa.Field(coerce=True, nullable=True, eq=1)
#     # title: Series[str] = pa.Field(coerce=True)

#     # TODO: Use these later in the pipeline when IDs have been extracted from dicts.
#     # @pa.check("route", groupby="plan", name="many_to_one_plan_route")
#     # def check_many_to_one_plan_route(
#     #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
#     # ) -> bool:
#     #     """Check that each plan has only one route."""
#     #     # NOTE: route is dict and unhashable, so the reciprocal check is not possible.
#     #     return all(route.nunique() == 1 for route in grouped_value.values())

#     # @pa.check("plan", groupby="route", name="many_to_one_route_plan")
#     # def check_many_to_one_route_plan(
#     #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
#     # ) -> bool:
#     #     """Check that each route has only one plan."""
#     #     return all(plan.nunique() == 1 for plan in grouped_value.values())

#     # @pa.check("route", groupby="id", name="many_to_one_stop_route")
#     # def check_many_to_one_stop_route(
#     #     cls, grouped_value: dict[str, Series[str]]  # noqa: B902
#     # ) -> bool:
#     #     """Check that each stop has only one route."""
#     #     return all(route.nunique() == 1 for route in grouped_value.values())

#     # @pa.check("plan", groupby="id", name="many_to_one_stop_plan")
#     def check_many_to_one_stop_plan(
#         cls, grouped_value: dict[str, Series[str]]  # noqa: B902
#     ) -> bool:
#         """Check that each stop has only one plan."""
#         return all(plan.nunique() == 1 for plan in grouped_value.values())

#     @pa.check("stopPosition", groupby="plan", name="stop_position_unique")
#     def check_stop_position_unique(
#         cls, grouped_value: dict[str, Series[str]]  # noqa: B902
#     ) -> bool:
#         """Check that each plan has unique stop positions."""
#         return all(
#             len(stopPosition) == stopPosition.nunique()
#             for stopPosition in grouped_value.values()
#         )

#     @pa.check("stopPosition", groupby="plan", name="stop_position_continguous")
#     def check_stop_position_continguous(
#         cls, grouped_value: dict[str, Series[str]]  # noqa: B902
#     ) -> bool:
#         """Check that each plan has contiguous stop positions."""
#         return all(
#             sorted(stopPosition.to_list()) == list(range(0, len(stopPosition.to_list())))
#             for stopPosition in grouped_value.values()
#         )
