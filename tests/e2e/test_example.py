"""This is just an example test."""

import time

import pytest

from reference_package import wait_a_second


@pytest.mark.parametrize("secs", [1, 2, 3])
def test_example(secs: int) -> None:
    """e2e tests are usually to test real, live stuff like DBs, filesystems, etc.

    We'll just make a simple unit test here of our example function.

    Test that we wait at least as long as we expect to wait.
    """
    start_time = time.time()
    wait_a_second(secs=secs)
    end_time = time.time()
    assert end_time - start_time > secs
