"""Top-level init."""

from importlib.metadata import version

from reference_package.api.public import wait_a_second

try:
    __version__: str = version(__name__)
except Exception:
    __version__ = "unknown"

del version
