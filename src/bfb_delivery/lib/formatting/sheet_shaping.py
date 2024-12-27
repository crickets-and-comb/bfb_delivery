"""Functions for shaping and formatting spreadsheets."""

from datetime import datetime
from pathlib import Path

import pandas as pd
from typeguard import typechecked

from bfb_delivery.lib.constants import COMBINED_ROUTES_COLUMNS, SPLIT_ROUTE_COLUMNS, Columns


# TODO: Order by apartment number.
# TODO: Get real input tables to verify this works.
# (Should match structure of split_chunked_route outputs.)
# TODO: Validate stop numbers?
@typechecked
def combine_route_tables(
    input_paths: list[Path | str], output_dir: Path | str, output_filename: str
) -> Path:
    """See public docstring: :py:func:`bfb_delivery.api.public.combine_route_tables`."""
    if len(input_paths) == 0:
        raise ValueError("input_paths must have at least one path.")

    paths = [Path(path) for path in input_paths]
    output_dir = Path(output_dir) if output_dir else paths[0].parent
    output_filename = (
        f"combined_routes_{datetime.now().strftime('%Y%m%d')}.xlsx"
        if output_filename == ""
        else output_filename
    )
    output_path = output_dir / output_filename
    output_dir.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path) as writer:
        for path in paths:
            df = pd.read_csv(path)
            df[COMBINED_ROUTES_COLUMNS].to_excel(writer, sheet_name=path.stem, index=False)

    return output_path.resolve()


# TODO: There's got to be a way to set the docstring as a constant.
# TODO: Use Pandera.
# TODO: Switch to or allow CSVs instead of Excel files.
@typechecked
def split_chunked_route(
    input_path: Path | str, output_dir: Path | str, output_filename: str, n_books: int
) -> list[Path]:
    """See public docstring: :py:func:`bfb_delivery.api.public.split_chunked_route`."""
    if n_books <= 0:
        raise ValueError("n_books must be greater than 0.")
    # TODO: Make this accept input_path only as Path?
    input_path = Path(input_path)

    chunked_sheet: pd.DataFrame = pd.read_excel(input_path)
    # TODO: Clean column names.
    # TODO: Validate columns? (Use Pandera?)

    drivers = chunked_sheet[Columns.DRIVER].unique()
    driver_count = len(drivers)
    if driver_count < n_books:
        raise ValueError(
            "n_books must be less than or equal to the number of drivers: "
            f"driver_count: {driver_count}, n_books: {n_books}."
        )

    output_dir = Path(output_dir) if output_dir else Path(input_path).parent
    base_output_filename = (
        f"split_workbook_{datetime.now().strftime('%Y%m%d')}.xlsx"
        if output_filename == ""
        else output_filename
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    split_workbook_paths: list[Path] = []
    driver_sets = [drivers[i::n_books] for i in range(n_books)]
    for i, driver_set in enumerate(driver_sets):
        i_file_name = f"{base_output_filename.split('.')[0]}_{i + 1}.xlsx"
        split_workbook_path: Path = output_dir / i_file_name
        split_workbook_paths.append(split_workbook_path)

        with pd.ExcelWriter(split_workbook_path) as writer:
            driver_set_df = chunked_sheet[chunked_sheet[Columns.DRIVER].isin(driver_set)]
            for driver, data in driver_set_df.groupby(Columns.DRIVER):
                data[SPLIT_ROUTE_COLUMNS].to_excel(
                    writer, sheet_name=str(driver), index=False
                )

    split_workbook_paths = [path.resolve() for path in split_workbook_paths]

    return split_workbook_paths
