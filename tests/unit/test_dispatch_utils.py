"""Test for the dispatch utils module."""

from typeguard import typechecked

from bfb_delivery.lib.dispatch.utils import get_circuit_key


@typechecked
def test_get_circuit_key(mock_dispatch_utils_circuit_key: str) -> None:
    """Test get_circuit_key function."""
    circuit_key = get_circuit_key()
    assert circuit_key == mock_dispatch_utils_circuit_key
