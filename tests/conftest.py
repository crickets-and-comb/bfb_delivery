"""Tests conftest."""

import os
from pathlib import Path
from typing import Any

import pytest


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
