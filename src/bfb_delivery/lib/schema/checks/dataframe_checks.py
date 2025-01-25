"""DataFrame checks."""

import pandas as pd
import pandera.extensions as extensions

from bfb_delivery.lib.constants import CircuitColumns, Columns, IntermediateColumns

# NOTE: Registering as dataframe checks instead of field checks includes the columns in the
# error message, whereas groupby field checks do not.
# NOTE: There may be better a way to mock an RDB structure (spin up temp DB), but this works.


@extensions.register_check_method(statistics=["flag"])
def at_least_one_in_group_sheet_plan(df: pd.DataFrame, flag: bool) -> bool:
    """Check that at least one value in a group is not null or empty."""
    return (
        at_least_one_in_group(
            df=df,
            group_col=IntermediateColumns.DRIVER_SHEET_NAME,
            at_least_one_col=CircuitColumns.PLAN,
        )
        if flag
        else True
    )


@extensions.register_check_method(statistics=["flag"])
def at_least_one_in_group_route_sheet(df: pd.DataFrame, flag: bool) -> bool:
    """Check that at least one value in a group is not null or empty."""
    return (
        at_least_one_in_group(
            df=df,
            group_col=CircuitColumns.ROUTE,
            at_least_one_col=IntermediateColumns.DRIVER_SHEET_NAME,
        )
        if flag
        else True
    )


@extensions.register_check_method(statistics=["flag"])
def at_least_one_in_group_sheet_route(df: pd.DataFrame, flag: bool) -> bool:
    """Check that at least one value in a group is not null or empty."""
    return (
        at_least_one_in_group(
            df=df,
            group_col=IntermediateColumns.DRIVER_SHEET_NAME,
            at_least_one_col=CircuitColumns.ROUTE,
        )
        if flag
        else True
    )


@extensions.register_check_method(statistics=["flag"])
def at_least_one_in_group_sheet_stop(df: pd.DataFrame, flag: bool) -> bool:
    """Check that at least one value in a group is not null or empty."""
    return (
        at_least_one_in_group(
            df=df,
            group_col=IntermediateColumns.DRIVER_SHEET_NAME,
            at_least_one_col=Columns.STOP_NO,
        )
        if flag
        else True
    )


@extensions.register_check_method(statistics=["group_col", "at_least_one_col"])
def at_least_one_in_group(df: pd.DataFrame, group_col: str, at_least_one_col: str) -> bool:
    """Check that at least one value in a group is not null or empty."""
    return all(df.groupby(group_col)[at_least_one_col].count() > 0)


@extensions.register_check_method(statistics=["group_col", "contiguous_col", "start_idx"])
def contiguous_group(
    df: pd.DataFrame, group_col: str, contiguous_col: str, start_idx: int
) -> bool:
    """Assert that values are contiguous in each group."""
    return all(
        sorted(vals) == list(range(start_idx, len(vals) + start_idx))
        for _, vals in df.groupby(group_col)[contiguous_col]
    )


@extensions.register_check_method(statistics=["col_a", "col_b"])
def equal_cols(df: pd.DataFrame, col_a: str, col_b: str) -> bool:
    """Assert that plan titles are the same as route titles."""
    return all(df[col_a] == df[col_b])


@extensions.register_check_method(statistics=["cols"])
def increasing_by(df: pd.DataFrame, cols: list[str]) -> bool:
    """Assert that a DataFrame is sorted by columns."""

    def increasing_groupby(df: pd.DataFrame, col_a: str, col_b: str) -> bool:
        return df[col_a].is_monotonic_increasing and all(
            series.is_monotonic_increasing for _, series in df.groupby(col_a)[col_b]
        )

    return all(
        increasing_groupby(df, col_a=cols[i], col_b=cols[i + 1]) for i in range(len(cols) - 1)
    )


@extensions.register_check_method(statistics=["flag"])
def address1_in_address(df: pd.DataFrame, flag: bool) -> bool:
    """Check that a address line one in address."""
    return (
        item_in_dict_col(
            df=df, col_name=CircuitColumns.ADDRESS, item_name=CircuitColumns.ADDRESS_LINE_1
        )
        if flag
        else True
    )


@extensions.register_check_method(statistics=["flag"])
def address2_in_address(df: pd.DataFrame, flag: bool) -> bool:
    """Check that a address line two in address."""
    return (
        item_in_dict_col(
            df=df, col_name=CircuitColumns.ADDRESS, item_name=CircuitColumns.ADDRESS_LINE_2
        )
        if flag
        else True
    )


@extensions.register_check_method(statistics=["col_name", "item_name"])
def item_in_dict_col(df: pd.DataFrame, col_name: str, item_name: str) -> bool:
    """Check that a dictionary field has an item in it."""
    return all(item_name in val.keys() for val in df[col_name])


@extensions.register_check_method(statistics=["many_col", "one_col"])
def many_to_one(df: pd.DataFrame, many_col: str, one_col: str) -> bool:
    """Assert that a column has a many-to-one relationship with another column."""
    return df.groupby(many_col)[one_col].nunique().eq(1).all()


@extensions.register_check_method(statistics=["flag"])
def one_to_one_route_sheet(df: pd.DataFrame, flag: bool) -> bool:
    """Assert that columns have a 1:1 relationship."""
    return (
        one_to_one(
            df=df, col_a=CircuitColumns.ROUTE, col_b=IntermediateColumns.DRIVER_SHEET_NAME
        )
        if flag
        else True
    )


@extensions.register_check_method(statistics=["col_a", "col_b"])
def one_to_one(df: pd.DataFrame, col_a: str, col_b: str) -> bool:
    """Assert that columns have a 1:1 relationship."""
    return (
        df.groupby(col_a)[col_b].nunique().eq(1).all()
        and df.groupby(col_b)[col_a].nunique().eq(1).all()
    )


@extensions.register_check_method(statistics=["group_col", "unique_col"])
def unique_group(df: pd.DataFrame, group_col: str, unique_col: str) -> bool:
    """Assert that values are unique in each group."""
    return all(len(vals) == vals.nunique() for _, vals in df.groupby(group_col)[unique_col])
