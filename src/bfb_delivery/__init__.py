"""Top-level init."""

from importlib.metadata import version

from bfb_delivery.api import combine_route_tables, format_combined_routes, split_chunked_route

try:
    __version__: str = version(__name__)
except Exception:
    __version__ = "unknown"

del version
