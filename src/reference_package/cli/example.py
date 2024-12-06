"""This is just an example."""

import click
from typeguard import typechecked

from reference_package.lib import example


@click.command()
@click.option("--secs", type=int, required=False, default=1)
@typechecked
def main(secs: int = 1) -> None:
    """Wait n seconds."""
    example.wait_a_second(secs=secs)
