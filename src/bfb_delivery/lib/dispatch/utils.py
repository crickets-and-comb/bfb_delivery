"""Utility functions for the dispatch module."""

import os

from dotenv import load_dotenv
from typeguard import typechecked


@typechecked
def get_circuit_key() -> str:
    """Get the Circuit API key."""
    load_dotenv()
    key = os.getenv("CIRCUIT_API_KEY")
    if not key:
        raise ValueError(
            "Circuit API key not found. Set the CIRCUIT_API_KEY environment variable."
        )

    return key
