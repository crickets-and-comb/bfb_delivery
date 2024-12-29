"""Functions for shaping and formatting spreadsheets."""

# TODO: When wrapping in final function, start calling it "make_manifest" or similar.
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.worksheet import Worksheet
from typeguard import typechecked

from bfb_delivery.lib.constants import (
    COMBINED_ROUTES_COLUMNS,
    FORMATTED_ROUTES_COLUMNS,
    PROTEIN_BOX_TYPES,
    SPLIT_ROUTE_COLUMNS,
    CellColors,
    Columns,
)
from bfb_delivery.lib.formatting.data_cleaning import (
    format_and_validate_data,
    format_column_names,
)


# TODO: Reoganize functions for workflow order.
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
        # TODO: Sort by driver and stop number.
        for path in paths:
            route_df = pd.read_csv(path)
            driver_name = path.stem
            route_df[COMBINED_ROUTES_COLUMNS].to_excel(
                writer, sheet_name=driver_name, index=False
            )

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
    # TODO: Make this accept input_path only as Path? Or only as str to simplify?
    input_path = Path(input_path)

    chunked_sheet: pd.DataFrame = pd.read_excel(input_path)
    chunked_sheet.columns = format_column_names(columns=chunked_sheet.columns.to_list())
    # TODO: Wrap for this use case so we can test in isolation?
    format_and_validate_data(df=chunked_sheet, columns=SPLIT_ROUTE_COLUMNS + [Columns.DRIVER])
    chunked_sheet.sort_values(by=[Columns.DRIVER, Columns.STOP_NO], inplace=True)
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
            # TODO: Sort by driver and stop number.
            for driver_name, data in driver_set_df.groupby(Columns.DRIVER):
                data[SPLIT_ROUTE_COLUMNS].to_excel(
                    writer, sheet_name=str(driver_name), index=False
                )

    split_workbook_paths = [path.resolve() for path in split_workbook_paths]

    return split_workbook_paths


@typechecked
def format_combined_routes(
    input_path: Path | str, output_dir: Path | str = "", output_filename: str = ""
) -> Path:
    """See public docstring: :py:func:`bfb_delivery.api.public.format_combined_routes`."""
    input_path = Path(input_path)
    output_dir = Path(output_dir) if output_dir else input_path.parent
    output_filename = (
        f"formatted_routes_{datetime.now().strftime('%Y%m%d')}.xlsx"
        if output_filename == ""
        else output_filename
    )
    output_path = Path(output_dir) / output_filename

    wb = Workbook()
    wb.remove(wb["Sheet"])
    # TODO: Pass in date as argument from CLI.
    date = "Dummy date"
    with pd.ExcelFile(input_path) as xls:
        for sheet_idx, sheet_name in enumerate(sorted(xls.sheet_names)):
            driver_name = str(sheet_name)
            route_df = pd.read_excel(xls, driver_name)
            route_df.columns = format_column_names(columns=route_df.columns.to_list())
            # TODO: Drop columns. (Set the constant?)
            # TODO: Use Pandera?
            format_and_validate_data(df=route_df, columns=COMBINED_ROUTES_COLUMNS)
            # TODO: Order by apartment number, and redo stop numbers?
            # May need to postpone this.
            # Or, for now, just do it if the apartments are already in contiguous stops.
            # Or if discontinuous, just regroup and bump the following stops.
            # Also, may not make the most sense in order of apt number. Ask team.
            route_df.sort_values(by=[Columns.STOP_NO], inplace=True)

            agg_dict = _aggregate_route_data(df=route_df)
            # TODO: !! What happens when there are more than one order for a stop? Two rows?
            # (Since order count column is dropped in manifest)
            # Oh wait, they're all 1s, so is that just a way for them to count them with sum?
            # If that's so, ignore it or validate always a 1?

            ws = wb.create_sheet(title=driver_name, index=sheet_idx)
            _add_header_row(ws=ws)
            _add_aggregate_block(ws=ws, agg_dict=agg_dict, date=date, driver_name=driver_name)
            _write_data_to_sheet(ws=ws, df=route_df)

            # TODO: Set column widths by df. (May need to write df before other cells.)
            # TODO: Word wrap notes (and neighborhoods?)

            # TODO: Add date to sheet name.
            # TODO: Append and format as we go instead.
            # TODO: Set print_area (Use calculate_dimensions)
            # TODO: set_printer_settings(paper_size, orientation)
        # TODO: Write a test that at least checks that the sheets are not empty.
    wb.save(output_path)

    return output_path.resolve()


@typechecked
def _aggregate_route_data(df: pd.DataFrame) -> dict:
    """Aggregate data for a single route.

    Args:
        df: The route data to aggregate.

    Returns:
        Dictionary of aggregated data.
    """
    agg_dict = {
        "box_counts": df.groupby(Columns.BOX_TYPE)[Columns.ORDER_COUNT].sum().to_dict(),
        "total_box_count": df[Columns.ORDER_COUNT].sum(),
        "protein_box_count": df[df[Columns.BOX_TYPE].isin(PROTEIN_BOX_TYPES)][
            Columns.ORDER_COUNT
        ].sum(),
        "neighborhoods": df[Columns.NEIGHBORHOOD].unique().tolist(),
    }

    return agg_dict


@typechecked
def _add_header_row(ws: Worksheet) -> None:
    """Append a reusable formatted row to the worksheet."""
    font = Font(bold=True)
    alignment_left = Alignment(horizontal="left")
    alignment_right = Alignment(horizontal="right")
    fill = PatternFill(
        start_color=CellColors.HEADER, end_color=CellColors.HEADER, fill_type="solid"
    )

    formatted_row = [
        {
            "value": "DRIVER SUPPORT: 555-555-5555",
            "font": font,
            "alignment": alignment_left,
            "fill": fill,
        },
        {"value": "", "font": font, "alignment": None, "fill": fill},
        {"value": "", "font": font, "alignment": None, "fill": fill},
        {
            "value": "RECIPIENT SUPPORT: 555-555-5555 X5",
            "font": font,
            "alignment": alignment_right,
            "fill": fill,
        },
        {"value": "", "font": font, "alignment": None, "fill": fill},
        {
            "value": "PLEASE SHRED MANIFEST AFTER COMPLETING ROUTE",
            "font": font,
            "alignment": alignment_right,
            "fill": fill,
        },
    ]

    for col_idx, col_data in enumerate(formatted_row, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_data["value"])
        cell.font = col_data["font"]
        if col_data["alignment"]:
            cell.alignment = col_data["alignment"]
        if col_data["fill"]:
            cell.fill = col_data["fill"]

    return


@typechecked
def _add_aggregate_block(ws: Worksheet, agg_dict: dict, date: str, driver_name: str) -> None:
    """Append left and right blocks to the worksheet row by row."""
    # TODO: Yeah, let's use an enum for box types since the manifest is a contract.
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    right_block = [
        [{"value": None, "fill": None, "border": None}],
        [
            {
                "value": "BASIC",
                "fill": PatternFill(
                    start_color=CellColors.BASIC,
                    end_color=CellColors.BASIC,
                    fill_type="solid",
                ),
                "border": thin_border,
            },
            {
                "value": agg_dict["box_counts"].get("BASIC", 0),
                "fill": None,
                "border": thin_border,
            },
        ],
        [
            {
                "value": "LA",
                "fill": PatternFill(
                    start_color=CellColors.LA, end_color=CellColors.LA, fill_type="solid"
                ),
                "border": thin_border,
            },
            {
                "value": agg_dict["box_counts"].get("LA", 0),
                "fill": None,
                "border": thin_border,
            },
        ],
        [
            {
                "value": "GF",
                "fill": PatternFill(
                    start_color=CellColors.GF, end_color=CellColors.GF, fill_type="solid"
                ),
                "border": thin_border,
            },
            {
                "value": agg_dict["box_counts"].get("GF", 0),
                "fill": None,
                "border": thin_border,
            },
        ],
        [
            {
                "value": "VEGAN",
                "fill": PatternFill(
                    start_color=CellColors.VEGAN,
                    end_color=CellColors.VEGAN,
                    fill_type="solid",
                ),
                "border": thin_border,
            },
            {
                "value": agg_dict["box_counts"].get("VEGAN", 0),
                "fill": None,
                "border": thin_border,
            },
        ],
        [
            {"value": "TOTAL BOX COUNT", "fill": None, "border": None},
            {"value": agg_dict["total_box_count"], "fill": None, "border": None},
        ],
        [
            {"value": "PROTEIN COUNT", "fill": None, "border": None},
            {"value": agg_dict["protein_box_count"], "fill": None, "border": None},
        ],
    ]

    left_block = [
        [{"value": None}],
        [{"value": f"Date: {date}"}],
        [{"value": None}],
        [{"value": f"Driver: {driver_name}"}],
        [{"value": None}],
        [{"value": f"Neighborhoods: {", ".join(agg_dict['neighborhoods'])}"}],
        [{"value": None}],
    ]

    bold_font = Font(bold=True)
    alignment_left = Alignment(horizontal="left")
    alignment_right = Alignment(horizontal="right")

    for i, (left_row, right_row) in enumerate(
        zip(left_block, right_block, strict=True), start=2
    ):
        for col_idx, cell_definition in enumerate(left_row, start=1):
            cell = ws.cell(row=i, column=col_idx, value=cell_definition["value"])
            cell.font = bold_font
            cell.alignment = alignment_left

        for col_idx, cell_definition in enumerate(right_row, start=5):
            cell = ws.cell(row=i, column=col_idx, value=cell_definition["value"])
            cell.font = bold_font
            cell.alignment = alignment_right
            if isinstance(cell_definition["fill"], PatternFill):
                cell.fill = cell_definition["fill"]
            if isinstance(cell_definition["border"], Border):
                cell.border = cell_definition["border"]

    return


@typechecked
def _write_data_to_sheet(ws: Worksheet, df: pd.DataFrame) -> None:
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    header_font = Font(bold=True)

    df_header_row_number = 9

    for r_idx, row in enumerate(
        dataframe_to_rows(df[FORMATTED_ROUTES_COLUMNS], index=False, header=True),
        start=df_header_row_number,
    ):
        for c_idx, value in enumerate(row, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.border = thin_border
            if r_idx == df_header_row_number:
                cell.font = header_font
                cell.alignment = Alignment(horizontal="left")

    return
