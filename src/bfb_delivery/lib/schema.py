"""The data schema for ETL steps."""

import pandera as pa
from pandera.typing import Series


class CircuitPlans(pa.DataFrameModel):
    """The schema for the Circuit plans data."""

    id: Series[str] = pa.Field(coerce=True, unique=True)
    title: Series[str] = pa.Field(coerce=True, unique=True)


class CircuitPlansFromDict(CircuitPlans):
    """The schema for the Circuit plans data from a dict."""

    class Config:
        """The configuration for the schema."""

        from_format = "dict"
