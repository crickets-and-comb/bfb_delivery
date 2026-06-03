"""Tests conftest."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from typeguard import typechecked

from bfb_delivery.lib.dispatch.api_callers import CustomStopPropertiesGetter


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark test types."""
    unit_tests_dir = str(config.rootpath / "tests" / "unit")
    integration_tests_dir = str(config.rootpath / "tests" / "integration")
    e2e_tests_dir = str(config.rootpath / "tests" / "e2e")

    for item in items:
        test_path = str(item.fspath)
        if test_path.startswith(unit_tests_dir):
            item.add_marker("unit")
        elif test_path.startswith(integration_tests_dir):
            item.add_marker("integration")
        elif test_path.startswith(e2e_tests_dir):
            item.add_marker("e2e")


@pytest.fixture(autouse=True)
def mock_custom_stop_properties_getter_make_call() -> Iterator:
    """Mock the API call in CustomStopPropertiesGetter."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "customStopProperties": [
            {
                "id": "aJyl6WxtFXXPXPt-7pGa2",
                "name": "protein",
            },
        ]
    }
    response.raise_for_status.return_value = None

    class MockCustomStopPropertiesGetter(CustomStopPropertiesGetter):
        """Mock CustomStopPropertiesGetter to use the mock response."""

        def _make_call(self) -> None:
            self._response = response

    mock_getter = MockCustomStopPropertiesGetter()
    mock_getter.call_api()

    with patch(
        "bfb_delivery.lib.utils.get_custom_stop_properties_getter", return_value=mock_getter
    ), patch(
        "bfb_delivery.lib.dispatch.read_circuit.get_custom_stop_properties_getter",
        return_value=mock_getter,
    ), patch(
        "bfb_delivery.lib.dispatch.write_to_circuit.get_custom_stop_properties_getter",
        return_value=mock_getter,
    ):
        from bfb_delivery.lib.utils import get_custom_stop_properties_getter

        get_custom_stop_properties_getter.cache_clear()
        yield
        get_custom_stop_properties_getter.cache_clear()


@pytest.fixture
def mock_dispatch_utils_circuit_key() -> str:
    """Fake Circuit API key."""
    return "Mock Dispatch Utils Circuit Key"


@pytest.fixture(autouse=True)
@typechecked
def mock_get_circuit_key_dispatch_utils(
    mock_dispatch_utils_circuit_key: str, tmp_path: Path
) -> Iterator:
    """Mock get_circuit_key."""
    env_path = tmp_path / ".env"
    env_path.write_text(f"CIRCUIT_API_KEY={mock_dispatch_utils_circuit_key}")

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
