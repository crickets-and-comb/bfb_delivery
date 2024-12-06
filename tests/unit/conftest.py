"""Conftest for unit tests."""

import pytest
from click.testing import CliRunner


@pytest.fixture()
def cli_runner() -> CliRunner:
    """Get a CliRunner."""
    return CliRunner()
