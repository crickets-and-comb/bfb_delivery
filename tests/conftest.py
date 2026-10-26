"""Tests conftest."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch

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


@pytest.fixture(scope="class", autouse=True)
def mock_custom_stop_properties_request_call() -> Iterator[Mock]:
    """Mock only the _request_call made by CustomStopPropertiesGetter."""
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

    mock_request_call = Mock(return_value=response)

    from bfb_delivery.lib.utils import get_custom_stop_properties_getter

    get_custom_stop_properties_getter.cache_clear()

    try:
        with patch.object(
            CustomStopPropertiesGetter,
            "_request_call",
            new_callable=PropertyMock,
        ) as mock_property:
            mock_property.return_value = mock_request_call
            yield mock_request_call
    finally:
        get_custom_stop_properties_getter.cache_clear()


@pytest.fixture
def mock_dispatch_utils_spoke_key() -> str:
    """Fake Spoke API key."""
    return "Mock Dispatch Utils Spoke Key"


@pytest.fixture(autouse=True)
@typechecked
def mock_get_spoke_key_dispatch_utils(
    mock_dispatch_utils_spoke_key: str, tmp_path: Path
) -> Iterator:
    """Mock get_spoke_key."""
    env_path = tmp_path / ".env"
    env_path.write_text(f"SPOKE_API_KEY={mock_dispatch_utils_spoke_key}")

    with patch(
        "bfb_delivery.lib.dispatch.utils.os_getcwd", return_value=tmp_path
    ) as mock_getcwd:
        mock_getcwd.return_value = tmp_path
        yield


@pytest.fixture(autouse=True)
@typechecked
def mock_get_spoke_key_api_callers() -> Iterator:
    """Mock get_spoke_key."""
    with patch(
        "bfb_delivery.lib.dispatch.api_callers.get_spoke_key", return_value="caller_key"
    ):
        yield
