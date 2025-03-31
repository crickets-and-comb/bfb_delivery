"""Tests conftest."""

import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest
from typeguard import typechecked


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark test types."""
    unit_tests_dir = os.path.join(config.rootdir, Path("tests/unit"))
    integration_tests_dir = os.path.join(config.rootdir, Path("tests/integration"))
    e2e_tests_dir = os.path.join(config.rootdir, Path("tests/e2e"))

    for item in items:
        test_path = str(item.fspath)
        if test_path.startswith(unit_tests_dir):
            item.add_marker("unit")
        elif test_path.startswith(integration_tests_dir):
            item.add_marker("integration")
        elif test_path.startswith(e2e_tests_dir):
            item.add_marker("e2e")


@pytest.fixture
def mock_key() -> str:
    """Fake Circuit API key."""
    return "dispatch_utils_key"


@pytest.fixture(autouse=True)
@typechecked
def mock_get_circuit_key_dispatch_utils(mock_key: str, tmp_path: Path) -> Iterator:
    """Mock get_circuit_key."""
    env_path = tmp_path / ".env"
    env_path.write_text(f"CIRCUIT_API_KEY={mock_key}")
    with patch(
        "bfb_delivery.lib.dispatch.utils.os_getcwd", return_value=tmp_path
    ) as mock_getcwd:
        mock_getcwd.return_value = tmp_path
        yield


@pytest.fixture(autouse=True)
@typechecked
def mock_get_circuit_key_api_callers() -> Iterator:
    """Mock get_circuit_key."""
    with patch(
        "bfb_delivery.lib.dispatch.api_callers.get_circuit_key", return_value="caller_key"
    ):
        yield
