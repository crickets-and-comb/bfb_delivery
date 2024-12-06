"""This is just an example test."""

import time

from click.testing import CliRunner

from reference_package.cli import example


def test_example(cli_runner: CliRunner) -> None:
    """Test that we wait at least as long as we expect to wait, using CLI."""
    start_time = time.time()
    cli_runner.invoke(example.main)
    end_time = time.time()
    assert end_time - start_time > 1
