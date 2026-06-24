"""Replace each driver with the supplied name, plus "#n".

Allows quick repurposing of production input file for testing/debugging.
Using "{driver} #{n}" keeps each route unique while suggesting the same driver at runtime.
"""

import logging

import click
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--input_fp",
    type=str,
    required=True,
    help="The file path to the input file to be modified. Must be a .xlsx file.",
)
@click.option(
    "--output_fp",
    type=str,
    required=True,
    help="The file path to the output file to be written. Must be a .xlsx file.",
)
@click.option(
    "--driver_name",
    type=str,
    required=False,
    default="Kaleb",
    help=(
        "The name to replace each driver with."
        " The script will append '# n' to each name, where n is the driver number."
    ),
)
@click.option(
    "--driver_col",
    type=str,
    required=False,
    default="Driver",
    help="The name of the column containing driver names. Default is 'driver_name'.",
)
def main(
    input_fp: str, output_fp: str, driver_name: str = "Kaleb", driver_col: str = "Driver"
) -> None:
    """Replace each driver with the supplied name, plus "# n"."""
    df = pd.read_excel(input_fp)
    drivers = df[driver_col].unique()
    for i, driver in enumerate(drivers):
        df.loc[df[driver_col] == driver, driver_col] = f"{driver_name} #{i + 1}"
    df.to_excel(output_fp, index=False)
    logger.info(f"Replaced {len(drivers)} drivers and wrote output to {output_fp}.")

    return


if __name__ == "__main__":
    main()
