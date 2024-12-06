"""Internal functions overlay library and are typically wrapped by public functions.

This allows us to maintain a separation of API from implementation.
Internal functions may come with extra options that public functions don't have, say for
power users and developers who may want to use an existing DB session or something.
"""

from typeguard import typechecked

from reference_package.lib import example


@typechecked
def wait_a_second(
    secs: int = 1, extra_string: str = "Fancy me, calling internal functions."
) -> None:
    """Just wait a second, or however many seconds you want.

    Also prints a message with the number you passed, along with any extra message you want.

    Arguments:
        secs: How many seconds to wait.
        extra_string: Extra message to add on tail of existing message.
    """
    example.wait_a_second(secs=secs, extra_string=extra_string)
