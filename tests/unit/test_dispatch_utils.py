"""Test for the dispatch utils module."""

from typeguard import typechecked

from bfb_delivery.lib.dispatch.utils import get_circuit_key


@typechecked
def test_get_circuit_key(FAKE_KEY: str) -> None:
    """Test get_circuit_key function."""
    assert get_circuit_key() == FAKE_KEY
