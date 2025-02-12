"""Tests conftest."""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from typeguard import typechecked


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
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


def pytest_sessionfinish(session: Any, exitstatus: pytest.ExitCode) -> None:
    """Set as success if no tests are collected."""
    if exitstatus == pytest.ExitCode.NO_TESTS_COLLECTED:
        session.exitstatus = pytest.ExitCode.OK


@pytest.fixture(autouse=True)
@typechecked
def mock_get_circuit_key_dispatch_utils() -> Iterator:
    """Mock get_circuit_key."""
    with patch("bfb_delivery.lib.dispatch.utils.get_circuit_key", return_value="fakekey"):
        yield


@pytest.fixture(autouse=True)
@typechecked
def mock_get_circuit_key_api_callers() -> Iterator:
    """Mock get_circuit_key."""
    with patch("bfb_delivery.lib.dispatch.api_callers.get_circuit_key", return_value="fakekey"):
        yield