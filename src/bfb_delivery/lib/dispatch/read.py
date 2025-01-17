"""Read from Circuit."""

import os

from typeguard import typechecked

from bfb_delivery.lib.utils import get_friday


@typechecked
def get_route_files(date: str, output_dir: str) -> str:
    """Get the route files for the given date.

    Args:
        date: The date to get the routes for, as "YYYYMMDD".
            Empty string uses the soonest Friday.
        output_dir: The directory to save the routes to.
            Empty string saves to the present working directory.

    Returns:
        The path to the route files.
    """
    date = date if date else get_friday(fmt="%Y%m%d")
    output_dir = _get_output_dir(output_dir=output_dir)

    # TODO: Download/write route files from Circuit.

    return output_dir


@typechecked
def _get_output_dir(output_dir: str) -> str:
    """Get the output directory.

    Args:
        output_dir: The directory to save the routes to.
            Empty string saves to the present working directory.

    Returns:
        The output directory.
    """
    if not output_dir:
        # Get the present working directory.
        output_dir = os.getcwd()

    return output_dir
