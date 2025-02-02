"""Delete a plan from Circuit."""

import logging
from time import sleep

import click
import requests
from requests.auth import HTTPBasicAuth

from bfb_delivery.lib.constants import RateLimits
from bfb_delivery.lib.dispatch.utils import get_circuit_key, get_response_dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: Hm, plan not found, 404, but I didn't delete it.
# {'message': 'Route POST:/public/v0.2b/plans/plans/bwEIfsArXoNsRY7jf6uy not found', 'error': 'Not Found', 'statusCode': 404} # noqa
# Also: plans/yVQuIEwEySoEaQOzQ0QN, plans/IcHyv5Vd7E1gDG4j5126
# Did I create them successfully?
# If so, considered a live plan since the optimization status is "creating"?
# If a live plan, how can I find it other than to operate on it?
# {'id': 'plans/yVQuIEwEySoEaQOzQ0QN', 'title': '01.01 Kaleb #2', 'starts': {'day': 2, 'month': 2, 'year': 2025}, 'depot': 'depots/LsDXAC6SRAnYXcKoL3oH', 'distributed': False, 'writable': True, 'optimization': 'creating', 'drivers': [{'id': 'drivers/TAsfHJDOexFH3rMd2AQ5', 'name': 'Kaleb Coberly', 'email': 'kalebcoberly@gmail.com', 'phone': None, 'displayName': '', 'active': True}], 'routes': []} # noqa


@click.command()
@click.option(
    "--plan-id", type=str, required=True, help="The plan ID to be deleted. As 'plans/{id}'."
)
def main(plan_id: str, wait_seconds: float = RateLimits.WRITE_SECONDS) -> dict:
    """Delete a plan from Circuit."""
    response = requests.post(
        url=f"https://api.getcircuit.com/public/v0.2b/plans/{plan_id}",
        auth=HTTPBasicAuth(get_circuit_key(), ""),
        timeout=RateLimits.WRITE_TIMEOUT_SECONDS,
        json={},
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_e:
        response_dict = get_response_dict(response=response)
        err_msg = f"Got {response.status_code} reponse for {plan_id}: {response_dict}"
        raise requests.exceptions.HTTPError(err_msg) from http_e

    else:
        if response.status_code == 204:
            deletion = response.json()
        elif response.status_code == 429:
            wait_seconds = wait_seconds * 2
            logger.warning(f"Rate-limited. Waiting {wait_seconds} seconds to retry.")
            sleep(wait_seconds)
            deletion = main(plan_id=plan_id, wait_seconds=wait_seconds)
        else:
            response_dict = get_response_dict(response=response)
            raise ValueError(f"Unexpected response {response.status_code}: {response_dict}")

    logger.info(f"Plan {plan_id} deleted:\n{deletion}")

    return deletion


if __name__ == "__main__":
    main()


# Plans to delete:
# [{'id': 'plans/IcHyv5Vd7E1gDG4j5126',
#   'title': '01.01 Kaleb #1',
#   'starts': {'day': 2, 'month': 2, 'year': 2025},
#   'depot': 'depots/LsDXAC6SRAnYXcKoL3oH',
#   'distributed': False,
#   'writable': True,
#   'optimization': 'creating',
#   'drivers': [{'id': 'drivers/TAsfHJDOexFH3rMd2AQ5',
#                'name': 'Kaleb Coberly',
#                'email': 'kalebcoberly@gmail.com',
#                'phone': None,
#                'displayName': '',
#                'active': True}],
#   'routes': []},
#  {'id': 'plans/bwEIfsArXoNsRY7jf6uy',
#   'title': '01.01 Kaleb #1',
#   'starts': {'day': 2, 'month': 2, 'year': 2025},
#   'depot': 'depots/LsDXAC6SRAnYXcKoL3oH',
#   'distributed': False,
#   'writable': True,
#   'optimization': 'creating',
#   'drivers': [{'id': 'drivers/TAsfHJDOexFH3rMd2AQ5',
#                'name': 'Kaleb Coberly',
#                'email': 'kalebcoberly@gmail.com',
#                'phone': None,
#                'displayName': '',
#                'active': True}],
#   'routes': []},
#  {'id': 'plans/yVQuIEwEySoEaQOzQ0QN',
#   'title': '01.01 Kaleb #2',
#   'starts': {'day': 2, 'month': 2, 'year': 2025},
#   'depot': 'depots/LsDXAC6SRAnYXcKoL3oH',
#   'distributed': False,
#   'writable': True,
#   'optimization': 'creating',
#   'drivers': [{'id': 'drivers/TAsfHJDOexFH3rMd2AQ5',
#                'name': 'Kaleb Coberly',
#                'email': 'kalebcoberly@gmail.com',
#                'phone': None,
#                'displayName': '',
#                'active': True}],
#   'routes': []}]
