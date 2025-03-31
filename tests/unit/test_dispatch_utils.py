"""Test for the dispatch utils module."""

from logging import log

from typeguard import typechecked

from bfb_delivery.lib.dispatch.utils import get_circuit_key


@typechecked
def test_get_circuit_key(mock_key: str) -> None:
    """Test get_circuit_key function."""
    log(level=0, msg=f"mock_key: {mock_key}; get_circuit_key: {get_circuit_key()}")
    assert get_circuit_key() == mock_key
