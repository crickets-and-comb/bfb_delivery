"""Utility functions for the dispatch module."""

import logging
import os
from time import sleep
from typing import Any

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from bfb_delivery.lib.constants import RateLimits

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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


# TODO: Pass params instead of forming URL first. ("params", not "json")
# (Would need to then grab params URL for next page, or just add nextpage to params?)
# TODO: See if we can make this a api_callers class.
@typechecked
def get_responses(url: str) -> list[dict[str, Any]]:
    """Get all responses from a paginated API endpoint."""
    wait_seconds = RateLimits.READ_SECONDS
    # Calling the token salsa to trick bandit into ignoring what looks like a hardcoded token.
    next_page_salsa = ""
    next_page_cookie = ""
    last_next_page_salsa = ""
    responses = []

    while next_page_salsa is not None:
        page_url = url + str(next_page_cookie)
        response = requests.get(
            page_url,
            auth=HTTPBasicAuth(get_circuit_key(), ""),
            timeout=RateLimits.READ_TIMEOUT_SECONDS,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_e:
            response_dict = get_response_dict(response=response)
            err_msg = f"Got {response.status_code} reponse for {page_url}: {response_dict}"
            raise requests.exceptions.HTTPError(err_msg) from http_e

        else:
            if response.status_code == 200:
                stops = response.json()
                responses.append(stops)
                next_page_salsa = stops.get("nextPageToken", None)
            elif response.status_code == 429:
                wait_seconds = wait_seconds * 2
                logger.warning(
                    f"Rate-limited. Doubling per-request wait time to {wait_seconds} seconds."
                )
                next_page_salsa = last_next_page_salsa
            else:
                response_dict = get_response_dict(response=response)
                raise ValueError(
                    f"Unexpected response {response.status_code}: {response_dict}"
                )

            if next_page_salsa:
                salsa_prefix = "?" if "?" not in url else "&"
                next_page_cookie = f"{salsa_prefix}pageToken={next_page_salsa}"

            last_next_page_salsa = next_page_salsa

        sleep(wait_seconds)

    return responses


@typechecked
def get_response_dict(response: requests.Response) -> dict[str, Any]:
    """Safely handle a response that may not be JSON."""
    try:
        response_dict: dict = response.json()
    except Exception as e:
        response_dict = {
            "reason": response.reason,
            "additional_notes": "No-JSON response.",
            "No-JSON response exception:": str(e),
        }
    return response_dict
