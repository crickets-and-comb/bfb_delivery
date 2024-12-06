"""Public functions wrap internal functions which wrap library functions.

This allows separation of API from implementation. It also allows a simplified public API
separate from a more complex internal API with more options for power users.
"""

from typeguard import typechecked

from reference_package.api.internal import example


@typechecked
def wait_a_second(secs: int = 1) -> None:
    """Just wait a second, or however many seconds you want.

    Also prints a message with the number you passed.

    Arguments:
        secs: How many seconds to wait.
    """
    example.wait_a_second(secs=secs)
