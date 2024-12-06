"""This is an example module."""

import time


def wait_a_second(secs: int = 1, extra_string: str = "") -> None:
    """Just wait a second, or however many seconds you want.

    Also prints a message with the number you passed, along with any extra message you want.

    Arguments:
        secs: How many seconds to wait.
        extra_string: Extra message to add on tail of existing message.
    """
    print(f"Waiting {secs} seconds.{' ' + extra_string if extra_string else ''}")
    time.sleep(secs)
