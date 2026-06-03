"""Utility functions for the bfb_delivery module."""

from datetime import datetime
from functools import cache

import pandas as pd
from typeguard import typechecked

from bfb_delivery.lib.dispatch.api_callers import CustomStopPropertiesGetter


@typechecked
def get_friday(fmt: str) -> str:
    """Get the soonest Friday."""
    friday = datetime.now() + pd.DateOffset(weekday=4)
    return friday.strftime(fmt)


@cache
@typechecked
def get_custom_stop_properties_getter() -> CustomStopPropertiesGetter:
    """Get a cached instance of CustomStopPropertiesGetter."""
    getter = CustomStopPropertiesGetter()
    getter.call_api()
    return getter
