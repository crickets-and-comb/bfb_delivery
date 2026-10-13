"""Test for the dispatch utils module."""

from typeguard import typechecked

from bfb_delivery.lib.dispatch.utils import get_spoke_key


@typechecked
def test_get_spoke_key(mock_dispatch_utils_spoke_key: str) -> None:
    """Test get_spoke_key function."""
    # Seems to be subject to race condition when running `act`. Indeterminant failures.
    spoke_key = get_spoke_key()
    assert spoke_key == mock_dispatch_utils_spoke_key
