"""Conftest for unit tests."""

from collections.abc import Iterator
from unittest.mock import patch

import pytest
from click.testing import CliRunner


@pytest.fixture()
def cli_runner() -> CliRunner:
    """Get a CliRunner."""
    return CliRunner()


@pytest.fixture()
def mock_is_valid_number() -> Iterator:
    """Mock phonenumbers.is_valid_number as True."""
    with patch("phonenumbers.is_valid_number", return_value=True):
        yield
