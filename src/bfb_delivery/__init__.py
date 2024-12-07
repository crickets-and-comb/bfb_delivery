"""Top-level init."""

from importlib.metadata import version

from bfb_delivery.api.public import split_chunked_route, wait_a_second

try:
    __version__: str = version(__name__)
except Exception:
    __version__ = "unknown"

del version
