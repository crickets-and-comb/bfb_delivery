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
# (Would need to then grab params URL for next page?)
@typechecked
def get_responses(url: str) -> list[dict[str, Any]]:
    """Get all responses from a paginated API endpoint."""
    wait_seconds = RateLimits.READ_SECONDS
    next_page = ""
    responses = []

    while next_page is not None:
        # TODO: Pull this out more so we can test setting page_url and wait_seconds.
        page_url = url + str(next_page)
        response = requests.get(
            page_url,
            auth=HTTPBasicAuth(get_circuit_key(), ""),
            timeout=RateLimits.READ_TIMEOUT_SECONDS,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_e:
            response_dict = _get_response_dict(response=response)
            err_msg = f"Got {response.status_code} reponse for {page_url}: {response_dict}"
            raise requests.exceptions.HTTPError(err_msg) from http_e

        else:
            if response.status_code == 429:
                wait_seconds = wait_seconds * 2
                logger.warning(
                    f"Rate-limited. Doubling per-request wait time to {wait_seconds} seconds."
                )
            elif response.status_code == 200:
                stops = response.json()
                responses.append(stops)
                next_page = stops.get("nextPageToken", None)
            else:
                response_dict = _get_response_dict(response=response)
                raise ValueError(
                    f"Unexpected response {response.status_code}: {response_dict}"
                )

        if next_page:
            token_prefix = "?" if "?" not in url else "&"
            next_page = f"{token_prefix}pageToken={next_page}"

        sleep(wait_seconds)

    return responses


@typechecked
def _get_response_dict(response: requests.Response) -> dict[str, Any]:
    try:
        response_dict: dict = response.json()
    except Exception as e:
        response_dict = {
            "reason": response.reason,
            "additional_notes": "No-JSON response.",
            "No-JSON response exception:": str(e),
        }
    return response_dict
