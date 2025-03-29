"""Delete a plan from Circuit."""

import logging
from time import sleep

import click
import requests
from requests.auth import HTTPBasicAuth

# TODO: Move this up in inits. issue 59
from comb_utils.lib.api_callers import get_response_dict

from bfb_delivery.lib.constants import RateLimits
from bfb_delivery.lib.dispatch.utils import get_circuit_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--plan-id", type=str, required=True, help="The plan ID to be deleted. As 'plans/{id}'."
)
def main(plan_id: str, wait_seconds: float = RateLimits.WRITE_SECONDS) -> dict:
    """Delete a plan from Circuit."""
    response = requests.get(
        url=f"https://api.getcircuit.com/public/v0.2b/{plan_id}",
        auth=HTTPBasicAuth(get_circuit_key(), ""),
        timeout=RateLimits.WRITE_TIMEOUT_SECONDS,
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_e:
        response_dict = get_response_dict(response=response)
        err_msg = f"Got {response.status_code} reponse for {plan_id}: {response_dict}"
        raise requests.exceptions.HTTPError(err_msg) from http_e

    else:
        if response.status_code == 200:
            plan = response.json()
        elif response.status_code == 429:
            wait_seconds = wait_seconds * 2
            logger.warning(f"Rate-limited. Waiting {wait_seconds} seconds to retry.")
            sleep(wait_seconds)
            plan = main(plan_id=plan_id, wait_seconds=wait_seconds)
        else:
            response_dict = get_response_dict(response=response)
            raise ValueError(f"Unexpected response {response.status_code}: {response_dict}")

    logger.info(f"Plan {plan_id} retrieved:\n{plan}")

    return plan


if __name__ == "__main__":
    main()
