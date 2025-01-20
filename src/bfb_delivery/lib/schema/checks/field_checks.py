"""Field checks."""

import pandas as pd
import pandera.extensions as extensions


@extensions.register_check_method(statistics=["flag"])
def at_least_two_words(pandas_obj: pd.Series, flag: bool) -> bool:
    """Check that a string has at least two words."""
    return all(len(val.split()) >= 2 for val in pandas_obj) if flag else True


@extensions.register_check_method(statistics=["category_list"])
def in_list_case_insensitive(pandas_obj: pd.Series, *, category_list: list[str]) -> bool:
    """Check that a column is in a list."""
    return pandas_obj.str.upper().isin([val.upper() for val in category_list]).all()
