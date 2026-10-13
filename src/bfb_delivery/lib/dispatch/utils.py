"""Utility functions for the dispatch module."""

import logging
import os
from os import getcwd as os_getcwd  # For test patching.
from pathlib import Path

from dotenv import load_dotenv
from typeguard import typechecked

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@typechecked
def get_spoke_key() -> str:
    """Get the Spoke API key."""
    load_dotenv(dotenv_path=Path(os_getcwd()) / ".env", override=True)

    key = os.getenv("SPOKE_API_KEY")
    if not key:
        raise ValueError(
            "Spoke API key not found. Set the SPOKE_API_KEY environment variable."
        )

    return key
