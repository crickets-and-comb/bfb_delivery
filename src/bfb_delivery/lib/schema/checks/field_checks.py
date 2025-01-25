"""Field checks."""

import pandas as pd
import pandera.extensions as extensions

from bfb_delivery.lib.constants import CircuitColumns


@extensions.register_check_method(statistics=["flag"])
def at_least_two_words(pandas_obj: pd.Series, flag: bool) -> bool:
    """Check that a string has at least two words."""
    return all(len(val.split(" ")) >= 2 for val in pandas_obj) if flag else True


@extensions.register_check_method(statistics=["start_idx"])
def contiguous(pandas_obj: pd.Series, start_idx: int) -> bool:
    """Assert that values are contiguous."""
    return sorted(pandas_obj.to_list()) == list(
        range(start_idx, len(pandas_obj.to_list()) + start_idx)
    )


@extensions.register_check_method(statistics=["category_list"])
def in_list_case_insensitive(pandas_obj: pd.Series, *, category_list: list[str]) -> bool:
    """Check that a column is in a list."""
    return pandas_obj.str.upper().isin([val.upper() for val in category_list]).all()


@extensions.register_check_method(statistics=["flag"])
def is_list_of_one_or_less(pandas_obj: pd.Series, flag: bool) -> bool:
    """Check that a column is a list of one item."""
    return (
        all(isinstance(val, list) and 0 <= len(val) <= 1 for val in pandas_obj)
        if flag
        else True
    )


@extensions.register_check_method(statistics=["check_sort"])
def is_sorted(pandas_obj: pd.Series, check_sort: int) -> bool:
    """Assert that values are contiguous."""
    return sorted(pandas_obj.to_list()) == pandas_obj.to_list() if check_sort else True


@extensions.register_check_method(statistics=["item_name"])
def item_in_field_dict(pandas_obj: pd.Series, item_name: str) -> bool:
    """Check that a dictionary field has an item in it."""
    return all(item_name in val.keys() for val in pandas_obj)


@extensions.register_check_method(statistics=["flag"])
def one_product(pandas_obj: pd.Series, flag: bool) -> bool:
    """Ensure one and only one product per stop."""
    return (
        all(
            isinstance(val.get(CircuitColumns.PRODUCTS), list)
            and len(val.get(CircuitColumns.PRODUCTS)) == 1
            for val in pandas_obj
        )
        if flag
        else True
    )
