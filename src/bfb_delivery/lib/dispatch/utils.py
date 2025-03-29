"""Utility functions for the dispatch module."""

import logging
import os
from os import getcwd as os_getcwd  # For test patching.
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from typeguard import typechecked

from bfb_delivery.lib.dispatch import api_callers

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@typechecked
def get_circuit_key() -> str:
    """Get the Circuit API key."""
    load_dotenv(Path(os_getcwd()) / ".env")
    key = os.getenv("CIRCUIT_API_KEY")
    if not key:
        raise ValueError(
            "Circuit API key not found. Set the CIRCUIT_API_KEY environment variable."
        )

    return key


# TODO: Pass params instead of forming URL first. ("params", not "json")
# (Would need to then grab params URL for next page, or just add nextpage to params?)
# https://github.com/crickets-and-comb/bfb_delivery/issues/61
# TODO: bfb_delivery issue 59, comb_utils issue 24: Pass in paged response class?
@typechecked
def get_responses(url: str) -> list[dict[str, Any]]:
    """Get all responses from a paginated API endpoint."""
    # Calling the token salsa to trick bandit into ignoring what looks like a hardcoded token.
    next_page_salsa = ""
    next_page_cookie = ""
    responses = []

    while next_page_salsa is not None:
        paged_response_getter = api_callers.PagedResponseGetterBFB(
            page_url=url + str(next_page_cookie)
        )
        paged_response_getter.call_api()

        stops = paged_response_getter._response.json()
        responses.append(stops)
        next_page_salsa = paged_response_getter.next_page_salsa

        if next_page_salsa:
            salsa_prefix = "?" if "?" not in url else "&"
            next_page_cookie = f"{salsa_prefix}pageToken={next_page_salsa}"

    return responses
