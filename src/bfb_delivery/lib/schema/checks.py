"""Sschema checks."""

import pandas as pd
import pandera.extensions as extensions


@extensions.register_check_method(statistics=["many_col", "one_col"])
def many_to_one(df: pd.DataFrame, many_col: str, one_col: str) -> bool:
    """Assert that a column has a many-to-one relationship with another column."""
    return df.groupby(many_col)[one_col].nunique().eq(1).all()


@extensions.register_check_method(statistics=["col_a", "col_b"])
def one_to_one(df: pd.DataFrame, col_a: str, col_b: str) -> bool:
    """Assert that columns have a 1:1 relationship."""
    return (
        df.groupby(col_a)[col_b].nunique().eq(1).all()
        and df.groupby(col_b)[col_a].nunique().eq(1).all()  # noqa: W503
    )


@extensions.register_check_method(statistics=["group_col", "unique_col"])
def check_unique_group(df: pd.DataFrame, group_col: str, unique_col: str) -> bool:
    """Assert that values are unique in each group."""
    return all(len(vals) == vals.nunique() for _, vals in df.groupby(group_col)[unique_col])


@extensions.register_check_method(statistics=["group_col", "contiguous_col", "start_idx"])
def check_contiguous_group(
    df: pd.DataFrame, group_col: str, contiguous_col: str, start_idx: int
) -> bool:
    """Assert that values are contiguous in each group."""
    return all(
        sorted(vals) == list(range(start_idx, len(vals) + start_idx))
        for _, vals in df.groupby(group_col)[contiguous_col]
    )
