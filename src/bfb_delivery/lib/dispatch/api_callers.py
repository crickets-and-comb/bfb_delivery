"""Classes for making API calls."""

import logging
from collections.abc import Callable
from time import sleep
from typing import Any

import requests
from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from bfb_delivery.lib.constants import RateLimits
from bfb_delivery.lib.dispatch.utils import get_circuit_key, get_response_dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: See where else we can use this.
# TODO: Pass in verbose to silence rate-limit warnings.
class _BaseCaller:
    """A base class for making API calls."""

    # Must set in child class with _set*:
    _request_call: Callable  # requests.get or requests.post
    _url: str

    # Must set in child class:
    _timeout: float
    _min_wait_seconds: float
    _wait_seconds: float  # Adjusted by instances, at class level.

    # Optionally set in child class, to pass to _request_call if needed:
    _call_kwargs: dict[str, Any] = {}
    _wait_increase_scalar: float = 2
    _wait_decrease_scalar: float = 0.75

    # Set by object:
    _response: requests.Response
    _response_json: dict[str, Any]

    @typechecked
    def __init__(self, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize the APICaller.

        .. note::
            You must initialize _wait_seconds as a class variable in the child class.
            This allows child class instances to increase the wait time on rate limiting
            without passing between objects.
        """
        self._set_request_call()
        self._set_url()

    @typechecked
    def _set_request_call(self) -> None:
        """Set the request call method.

        requests.get or requests.post
        """
        raise NotImplementedError

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        raise NotImplementedError

    @typechecked
    def call_api(self) -> None:
        """Call the API."""
        self._call_api()
        self._decrease_wait_time()

    @typechecked
    def _call_api(self) -> None:
        """Call the API."""
        sleep(type(self)._wait_seconds)
        self._make_call()
        self._raise_for_status()
        self._parse_response()

    @typechecked
    def _make_call(self) -> None:
        self._response = self._request_call(
            url=self._url,
            auth=HTTPBasicAuth(get_circuit_key(), ""),
            timeout=self._timeout,
            **self._call_kwargs,
        )

    @typechecked
    def _raise_for_status(self) -> None:
        try:
            self._response.raise_for_status()
        except requests.exceptions.HTTPError as http_e:
            if self._response.status_code == 429:
                self._handle_429()
            elif self._response.status_code == 443:
                self._handle_443()
            else:
                response_dict = get_response_dict(response=self._response)
                err_msg = f"Got {self._response.status_code} reponse:\n{response_dict}"
                raise requests.exceptions.HTTPError(err_msg) from http_e

    @typechecked
    def _parse_response(self) -> None:
        if self._response.status_code == 200:
            self._handle_200()

        elif self._response.status_code == 204:
            self._handle_204()

        elif self._response.status_code == 429:
            # This is here as well as in the raise_for_status method because there was a case
            # when the status code was 429 but the response didn't raise.
            self._handle_429()
        else:
            response_dict = get_response_dict(response=self._response)
            raise ValueError(
                f"Unexpected response {self._response.status_code}:\n{response_dict}"
            )

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response."""
        self._response_json = self._response.json()

    @typechecked
    def _handle_204(self) -> None:
        """Handle a 204 response."""
        raise NotImplementedError

    @typechecked
    def _handle_429(self) -> None:
        """Handle a 429 response."""
        self._increase_wait_time()
        logger.warning(f"Rate limited. Waiting {type(self)._wait_seconds} seconds to retry.")
        self._call_api()

    @typechecked
    def _handle_443(self) -> None:
        """Handle a 443 response."""
        self._increase_timeout()
        response_dict = get_response_dict(response=self._response)
        logger.warning(
            f"Request timed out.\n{response_dict}"
            f"\nTrying again with longer timeout: {type(self)._timeout} seconds."
        )
        self._call_api()

    @typechecked
    def _decrease_wait_time(self) -> None:
        """Decrease the wait time on rate limiting, for all instances."""
        cls = type(self)
        cls._wait_seconds = max(
            cls._wait_seconds * self._wait_decrease_scalar, cls._min_wait_seconds
        )

    @typechecked
    def _increase_wait_time(self) -> None:
        """Increase the wait time on rate limiting, for all instances."""
        cls = type(self)
        cls._wait_seconds = cls._wait_seconds * self._wait_increase_scalar

    @typechecked
    def _increase_timeout(self) -> None:
        """Increase the timeout on rate limiting, for all instances."""
        cls = type(self)
        cls._timeout = cls._timeout * self._wait_increase_scalar


class _BaseGetCaller(_BaseCaller):
    """A class for making GET API calls."""

    _timeout: float = RateLimits.READ_TIMEOUT_SECONDS
    _min_wait_seconds: float = RateLimits.READ_SECONDS
    _wait_seconds: float = _min_wait_seconds

    @typechecked
    def _set_request_call(self) -> None:
        """Set the request call method."""
        self._request_call = requests.get


class _BasePostCaller(_BaseCaller):
    """A class for making POST API calls."""

    _timeout: float = RateLimits.WRITE_TIMEOUT_SECONDS
    _min_wait_seconds: float = RateLimits.WRITE_SECONDS
    _wait_seconds: float = _min_wait_seconds

    @typechecked
    def _set_request_call(self) -> None:
        """Set the request call method."""
        self._request_call = requests.post


class _BaseDeleteCaller(_BasePostCaller):
    """A class for making POST API calls."""

    @typechecked
    def _set_request_call(self) -> None:
        """Set the request call method."""
        self._request_call = requests.delete


# TODO: Check docs to see if init args show up, here and elsewhere.
class _BaseOptimizationCaller(_BaseCaller):
    """Base class for checking the status of an optimization."""

    operation_id: str
    finished: bool

    _min_wait_seconds: float = RateLimits.OPTIMIZATION_PER_SECOND
    _wait_seconds: float = _min_wait_seconds

    _plan_id: str
    _plan_title: str

    @typechecked
    def __init__(self, plan_id: str, plan_title: str, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize the OptimizationChecker object.

        Args:
            plan_id: The ID of the plan.
            plan_title: The title of the plan.
        """
        self._plan_id = plan_id
        self._plan_title = plan_title
        super().__init__()

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response."""
        super()._handle_200()

        if self._response_json["metadata"]["canceled"]:
            raise RuntimeError(
                f"Optimization canceled for {self._plan_title} ({self._plan_id}):"
                f"\n{self._response_json}"
            )
        if self._response_json.get("result"):
            if self._response_json["result"].get("skippedStops"):
                raise RuntimeError(
                    f"Skipped optimization stops for {self._plan_title} ({self._plan_id}):"
                    f"\n{self._response_json}"
                )
            if self._response_json["result"].get("code"):
                raise RuntimeError(
                    f"Errors in optimization for {self._plan_title} ({self._plan_id}):"
                    f"\n{self._response_json}"
                )

        self.operation_id = self._response_json["id"]
        self.finished = self._response_json = self._response_json["done"]


class PlanInitializer(_BasePostCaller):
    """Class for initializing plans."""

    _plan_data: dict

    @typechecked
    def __init__(self, plan_data: dict) -> None:
        """Initialize the PlanInitializer object.

        Args:
            plan_data: The data for the plan, to pass to `requests.post` `json` param.
        """
        self._plan_data = plan_data
        self._call_kwargs = {"json": plan_data}
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        self._url = "https://api.getcircuit.com/public/v0.2b/plans"


class StopUploader(_BasePostCaller):
    """Class for batch uploading stops."""

    stop_ids: list[str]

    _min_wait_seconds: float = RateLimits.BATCH_STOP_IMPORT_SECONDS
    _wait_seconds: float = _min_wait_seconds

    _plan_id: str
    _plan_title: str

    @typechecked
    def __init__(
        self,
        plan_id: str,
        plan_title: str,
        stop_array: list[dict[str, dict[str, str] | list[str] | int | str]],
    ) -> None:
        """Initialize the StopUploader object.

        Args:
            plan_id: The ID of the plan.
            plan_title: The title of the plan.
            stop_array: The array of stops to upload, to pass to `requests.post` `json` param.
        """
        self._plan_id = plan_id
        self._plan_title = plan_title
        self._stop_array = stop_array
        self._call_kwargs = {"json": stop_array}
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        self._url = f"https://api.getcircuit.com/public/v0.2b/{self._plan_id}/stops:import"

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response."""
        super()._handle_200()

        self.stop_ids = self._response_json["success"]
        failed = self._response_json.get("failed")
        if failed:
            raise RuntimeError(
                f"For {self._plan_title} ({self._plan_id}), failed to upload stops:\n{failed}"
            )
        elif len(self.stop_ids) != len(self._stop_array):
            raise RuntimeError(
                f"For {self._plan_title} ({self._plan_id}), did not upload same number of "
                f"stops as input:\n{self.stop_ids}\n{self._stop_array}"
            )


class OptimizationLauncher(_BaseOptimizationCaller, _BasePostCaller):
    """A class for launching route optimization."""

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        self._url = f"https://api.getcircuit.com/public/v0.2b/{self._plan_id}:optimize"


class OptimizationChecker(_BaseOptimizationCaller, _BaseGetCaller):
    """A class for checking the status of an optimization."""

    _min_wait_seconds: float = RateLimits.READ_SECONDS
    _wait_seconds: float = _min_wait_seconds

    @typechecked
    def __init__(self, plan_id: str, plan_title: str, operation_id: str) -> None:
        """Initialize the OptimizationChecker object.

        Args:
            plan_id: The ID of the plan.
            plan_title: The title of the plan.
            operation_id: The ID of the operation.
        """
        self.operation_id = operation_id
        super().__init__(plan_id=plan_id, plan_title=plan_title)

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        self._url = f"https://api.getcircuit.com/public/v0.2b/{self.operation_id}"


class PlanDistributor(_BasePostCaller):
    """Class for distributing plans."""

    _plan_id: str
    _plan_title: str

    @typechecked
    def __init__(self, plan_id: str, plan_title: str) -> None:
        """Initialize the PlanDistributor object.

        Args:
            plan_id: The ID of the plan.
            plan_title: The title of the plan.
        """
        self._plan_id = plan_id
        self._plan_title = plan_title
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        self._url = f"https://api.getcircuit.com/public/v0.2b/{self._plan_id}:distribute"

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response."""
        super()._handle_200()
        if not self._response_json["distributed"]:
            raise RuntimeError(
                f"Failed to distribute plan {self._plan_title} ({self._plan_id}):"
                f"\n{self._response_json}"
            )


class PlanDeleter(_BaseDeleteCaller):
    """Class for deleting plans."""

    deletion: bool

    @typechecked
    def __init__(self, plan_id: str) -> None:
        """Initialize the PlanDeleter object.

        Args:
            plan_id: The ID of the plan.
        """
        self._plan_id = plan_id
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        self._url = f"https://api.getcircuit.com/public/v0.2b/{self._plan_id}"

    @typechecked
    def _handle_204(self) -> None:
        """Handle a 204 response."""
        self.deletion = True
