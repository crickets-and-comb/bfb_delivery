"""Module for custom exceptions."""


class BFBError(Exception):
    """Base class for exceptions in this module."""

    pass


class AssignmentOutOfRange(BFBError):
    """Raised when a user selects a driver index out of range."""

    pass


class InactiveDriverAssignment(BFBError):
    """Raised when a user selects an inactive driver."""

    pass
