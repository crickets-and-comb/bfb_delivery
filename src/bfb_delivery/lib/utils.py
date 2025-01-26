"""Utility functions for the bfb_delivery module."""

from datetime import datetime

import pandas as pd
from typeguard import typechecked


@typechecked
def get_friday(fmt: str) -> str:
    """Get the soonest Friday."""
    friday = datetime.now() + pd.DateOffset(weekday=4)
    return friday.strftime(fmt)
