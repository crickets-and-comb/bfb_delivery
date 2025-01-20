"""The data schema for ETL steps."""

# TODO: Move to new folder. Split into checks and schema files.


from typing import Any, Self

import pandera as pa
from pandera.errors import SchemaError
from pandera.typing import Series
from pandera.typing.common import DataFrameBase


class NonVerboseDataFrameModel(pa.DataFrameModel):
    """A DataFrameModel that does not print verbose error message."""

    @classmethod
    def validate(
        cls, *args: tuple[Any, ...], **kwargs: dict[str, Any]
    ) -> DataFrameBase[Self]:
        """Validate the DataFrame without printing verbose error messages."""
        try:
            return super().validate(*args, **kwargs)
        except SchemaError as e:
            e_dict = vars(e)
            err_msg = "Error validating the raw routes DataFrame."
            schema = e_dict.get("schema")
            reason_code = e_dict.get("reason_code")
            column_name = e_dict.get("column_name")
            check = e_dict.get("check")
            failure_cases = e_dict.get("failure_cases")
            if schema:
                err_msg += f"\nSchema: {schema}"
            if reason_code:
                err_msg += f"\nReason code: {reason_code}"
            if column_name:
                err_msg += f"\nColumn name: {column_name}"
            if check:
                err_msg += f"\nCheck: {check}"
            breakpoint()
            raise SchemaError(schema=schema, data=failure_cases, message=err_msg) from e


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


class CircuitRoutesConcatOut(NonVerboseDataFrameModel):
    """The schema for the Circuit routes data."""

    # TODO: Recycle fields.

    # plan id e.g. "plans/0IWNayD8NEkvD5fQe2SQ":
    plan: Series[str] = pa.Field(coerce=True, str_startswith="plans/")
    # route id e.g. "routes/lITTnQsxYffqJQDxIpzr", but in dict at this step.
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
    # title: Series[str] = pa.Field(coerce=True)

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

    # @pa.check("plan", groupby="id", name="many_to_one_stop_plan")
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


# TODO: Do dict field validations on input to tx.
