"""Classes for making API calls."""

# TODO: Move this up to lib.

import logging
from abc import abstractmethod
from collections.abc import Callable
from time import sleep
from typing import Any

import requests
from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from bfb_delivery.lib.constants import CIRCUIT_URL, CircuitColumns, RateLimits
from bfb_delivery.lib.dispatch.utils import get_circuit_key, get_response_dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: Move base classes to comb_utils.


class BaseCaller:
    """An abstract class for making API calls.

    Example:
        .. code:: python

            class MyGetCaller(BaseCaller):
                target_response_value

                _min_wait_seconds: float = 0.1
                # Initialize _wait_seconds and timeout as a class variable.
                # Instances will adjust for class.
                _wait_seconds: float = _min_wait_seconds
                _timeout: float = 10

                def _set_request_call(self):
                    self._request_call = requests.get

                def _set_url(self):
                    self._url = "https://example.com/public/v0.2b/"

                def _handle_200(self):
                    super()._handle_200()
                    self.target_response_value = self.response_json["target_key"]

            my_caller = MyCaller()
            my_caller.call_api()
            target_response_value = my_caller.target_response_value

    .. important::
        You must initialize _wait_seconds, _timeout, and _min_wait_seconds in child classes.
        This allows child class instances to adjust the wait/timeout time for the child class.

    .. warning::
        There is a potential for this to run indefinitely for rate limiting and timeouts.
        It handles them somewhat intelligently, but the assumption is that someone is watching
        this run in the background and will stop it if it runs too long. It will eventually
        at least crash the memory, depending on available memory, mean time to failure, and
        time left in the universe.
    """

    # Set by object:
    #: The JSON from the response.
    response_json: dict[str, Any]
    #: The response from the API call.
    _response: requests.Response

    # Must set in child class with _set*:
    #: The requests call method. (get, post, etc.)
    _request_call: Callable
    #: The URL for the API call.
    _url: str

    # Must set in child class:
    #: The timeout for the API call.
    _timeout: float
    #: The minimum wait time between API calls.
    _min_wait_seconds: float
    #: The wait time between API calls. (Adjusted by instances, at class level.)
    _wait_seconds: float

    # Optionally set in child class, to pass to _request_call if needed:
    #: The kwargs to pass to the requests call.
    _call_kwargs: dict[str, Any] = {}
    #: The scalar to increase wait time on rate limiting.
    _wait_increase_scalar: float = RateLimits.WAIT_INCREASE_SCALAR
    #: The scalar to decrease wait time on success.
    _wait_decrease_scalar: float = RateLimits.WAIT_DECREASE_SECONDS

    @typechecked
    def __init__(self) -> None:  # noqa: ANN401
        """Initialize the BaseCaller object."""
        self._set_request_call()
        self._set_url()

    @abstractmethod
    @typechecked
    def _set_request_call(self) -> None:
        """Set the requests call method.

        requests.get, requests.post, etc.

        Raises:
            NotImplementedError: If not implemented in child class.
        """
        raise NotImplementedError

    @abstractmethod
    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call.

        Raises:
            NotImplementedError: If not implemented in child class.
        """
        raise NotImplementedError

    @typechecked
    def call_api(self) -> None:
        """The main method for making the API call.

        Handle errors, parse response, and decrease class wait time on success.

        Raises:
            ValueError: If the response status code is not expected.
            requests.exceptions.HTTPError: For non-rate-limiting errors.
        """
        # Separated to allow for recursive calls on rate limiting.
        self._call_api()
        self._decrease_wait_time()

    @typechecked
    def _call_api(self) -> None:
        """Wait and make and handle the API call.

        Wrapped separately to allow for recursive calls on rate limiting and timeout.
        """
        sleep(type(self)._wait_seconds)
        self._make_call()
        self._raise_for_status()
        self._parse_response()

    @typechecked
    def _make_call(self) -> None:
        """Make the API call."""
        self._response = self._request_call(
            url=self._url,
            auth=HTTPBasicAuth(get_circuit_key(), ""),
            timeout=self._timeout,
            **self._call_kwargs,
        )

    @typechecked
    def _raise_for_status(self) -> None:
        """Handle error responses.

        For 429 (rate limiting), increases wait time and recursively calls the API.
        For timeout, increases timeout and recursively calls the API.

        Raises:
            requests.exceptions.HTTPError: For non-rate-limiting errors.
            requests.exceptions.Timeout: For timeouts.
        """
        try:
            self._response.raise_for_status()
        except requests.exceptions.HTTPError as http_e:
            if self._response.status_code == 429:
                self._handle_429()
            else:
                self._handle_unknown_error(e=http_e)
        except requests.exceptions.Timeout:
            self._handle_timeout()

    @typechecked
    def _parse_response(self) -> None:
        """Parse the non-error reponse (200).

        Raises:
            ValueError: If the response status code is not expected.
        """
        if self._response.status_code == 200:
            self._handle_200()
        elif self._response.status_code == 204:
            self._handle_204()
        elif self._response.status_code == 429:
            # This is here as well as in the _raise_for_status method because there was a case
            # when the status code was 429 but the response didn't raise.
            self._handle_429()
        else:
            response_dict = get_response_dict(response=self._response)
            raise ValueError(
                f"Unexpected response {self._response.status_code}:\n{response_dict}"
            )

    @typechecked
    def _handle_429(self) -> None:
        """Handle a 429 response.

        Inreases the class wait time and recursively calls the API.
        """
        self._increase_wait_time()
        logger.warning(f"Rate limited. Waiting {type(self)._wait_seconds} seconds to retry.")
        self._call_api()

    @typechecked
    def _handle_timeout(self) -> None:
        """Handle a timeout response.

        Increases the class timeout and recursively calls the API.
        """
        self._increase_timeout()
        response_dict = get_response_dict(response=self._response)
        logger.warning(
            f"Request timed out.\n{response_dict}"
            f"\nTrying again with longer timeout: {type(self)._timeout} seconds."
        )
        self._call_api()

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response.

        Just gets the JSON from the response and sets it to `response_json`.
        """
        self.response_json = self._response.json()

    @typechecked
    def _handle_204(self) -> None:
        """Handle a 204 response.

        Just sets `response_json` to an empty dictionary.
        """
        self.response_json = {}

    @typechecked
    def _handle_unknown_error(self, e: Exception) -> None:
        """Handle an unknown error response.

        Raises:
            Exception: The original error.
        """
        response_dict = get_response_dict(response=self._response)
        err_msg = f"Got {self._response.status_code} response:\n{response_dict}"
        raise requests.exceptions.HTTPError(err_msg) from e

    @typechecked
    def _decrease_wait_time(self) -> None:
        """Decrease the wait time between API calls for whole class."""
        cls = type(self)
        cls._wait_seconds = max(
            cls._wait_seconds * self._wait_decrease_scalar, cls._min_wait_seconds
        )

    @typechecked
    def _increase_wait_time(self) -> None:
        """Increase the wait time between API calls for whole class."""
        cls = type(self)
        cls._wait_seconds = cls._wait_seconds * self._wait_increase_scalar

    @typechecked
    def _increase_timeout(self) -> None:
        """Increase the timeout for the API call for whole class."""
        cls = type(self)
        cls._timeout = cls._timeout * self._wait_increase_scalar


class BaseGetCaller(BaseCaller):
    """A base class for making GET API calls.

    Presets the timeout, initial wait time, and requests method.
    """

    _timeout: float = RateLimits.READ_TIMEOUT_SECONDS
    _min_wait_seconds: float = RateLimits.READ_SECONDS
    _wait_seconds: float = _min_wait_seconds

    @typechecked
    def _set_request_call(self) -> None:
        """Set the requests call method to `requests.get`."""
        self._request_call = requests.get


class BasePostCaller(BaseCaller):
    """A base class for making POST API calls.

    Presets the timeout, initial wait time, and requests method.
    """

    _timeout: float = RateLimits.WRITE_TIMEOUT_SECONDS
    _min_wait_seconds: float = RateLimits.WRITE_SECONDS
    _wait_seconds: float = _min_wait_seconds

    @typechecked
    def _set_request_call(self) -> None:
        """Set the requests call method to `requests.post`."""
        self._request_call = requests.post


class BaseDeleteCaller(BasePostCaller):
    """A base class for making DELETE API calls.

    Presets the timeout, initial wait time, and requests method.
    """

    @typechecked
    def _set_request_call(self) -> None:
        """Set the requests call method to `requests.delete`."""
        self._request_call = requests.delete


class BaseOptimizationCaller(BaseCaller):
    """A base class for checking the status of an optimization."""

    #: The ID of the operation.
    operation_id: str
    #: Whether the optimization is finished.
    finished: bool

    _timeout: float = RateLimits.WRITE_TIMEOUT_SECONDS
    _min_wait_seconds: float = RateLimits.OPTIMIZATION_PER_SECOND
    _wait_seconds: float = _min_wait_seconds

    #: The ID of the plan.
    _plan_id: str
    #: The title of the plan.
    _plan_title: str

    @typechecked
    def __init__(self, plan_id: str, plan_title: str) -> None:  # noqa: ANN401
        """Initialize the BaseOptimizationCaller object.

        Args:
            plan_id: The ID of the plan. (e.g. plans/asfoghaev)
            plan_title: The title of the plan.
        """
        self._plan_id = plan_id
        self._plan_title = plan_title
        super().__init__()

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response.

        Sets `operation_id` and whether the optimization is `finished`.

        Raises:
            RuntimeError: If the optimization was canceled, stops were skipped, or there were
                errors.
        """
        super()._handle_200()

        if self.response_json[CircuitColumns.METADATA][CircuitColumns.CANCELED]:
            raise RuntimeError(
                f"Optimization canceled for {self._plan_title} ({self._plan_id}):"
                f"\n{self.response_json}"
            )
        if self.response_json.get(CircuitColumns.RESULT):
            if self.response_json[CircuitColumns.RESULT].get(CircuitColumns.SKIPPED_STOPS):
                raise RuntimeError(
                    f"Skipped optimization stops for {self._plan_title} ({self._plan_id}):"
                    f"\n{self.response_json}"
                )
            if self.response_json[CircuitColumns.RESULT].get(CircuitColumns.CODE):
                raise RuntimeError(
                    f"Errors in optimization for {self._plan_title} ({self._plan_id}):"
                    f"\n{self.response_json}"
                )

        self.operation_id = self.response_json[CircuitColumns.ID]
        self.finished = self.response_json[CircuitColumns.DONE]


class PagedResponseGetter(BaseGetCaller):
    """Class for getting paged responses."""

    # The nextPageToken returned, but called salsa to avoid bandit.
    next_page_salsa: str | None

    #: The URL for the page.
    _page_url: str

    @typechecked
    def __init__(self, page_url: str) -> None:
        """Initialize the PagedResponseGetter object.

        Args:
            page_url: The URL for the page. (Optionally contains nextPageToken.)
        """
        self._page_url = page_url
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call to the `page_url`."""
        self._url = self._page_url

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response.

        Sets `next_page_salsa` to the nextPageToken.
        """
        super()._handle_200()
        self.next_page_salsa = self.response_json.get("nextPageToken", None)


class PlanInitializer(BasePostCaller):
    """Class for initializing plans."""

    #: The ID of the plan.
    plan_id: str
    #: Whether the plan is writeable.
    writable: bool

    #: The data dictionary for the plan.
    _plan_data: dict

    @typechecked
    def __init__(self, plan_data: dict) -> None:
        """Initialize the PlanInitializer object.

        Args:
            plan_data: The data dictionary for the plan.
                To pass to `requests.post` `json` param.
        """
        self._plan_data = plan_data
        self._call_kwargs = {"json": plan_data}
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call."""
        self._url = f"{CIRCUIT_URL}/plans"

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response.

        Sets `plan_id` and `writable`.
        """
        super()._handle_200()
        self.plan_id = self.response_json[CircuitColumns.ID]
        self.writable = self.response_json[CircuitColumns.WRITABLE]


class StopUploader(BasePostCaller):
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
            plan_id: The ID of the plan. (e.g. plans/asfoghaev)
            plan_title: The title of the plan.
            stop_array: The array of stops dictionaries to upload.
                To pass to `requests.post` `json` param.
        """
        self._plan_id = plan_id
        self._plan_title = plan_title
        self._stop_array = stop_array
        self._call_kwargs = {"json": stop_array}
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call with `plan_id`."""
        self._url = f"{CIRCUIT_URL}/{self._plan_id}/stops:import"

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response.

        Sets `stop_ids` to the successful stop IDs.

        Raises:
            RuntimeError: If stops failed to upload.
            RuntimeError: If the number of stops uploaded differs from input.
        """
        super()._handle_200()

        self.stop_ids = self.response_json["success"]
        failed = self.response_json.get("failed")
        if failed:
            raise RuntimeError(
                f"For {self._plan_title} ({self._plan_id}), failed to upload stops:\n{failed}"
            )
        elif len(self.stop_ids) != len(self._stop_array):
            raise RuntimeError(
                f"For {self._plan_title} ({self._plan_id}), did not upload same number of "
                f"stops as input:\n{self.stop_ids}\n{self._stop_array}"
            )


class OptimizationLauncher(BaseOptimizationCaller, BasePostCaller):
    """A class for launching route optimization.

    Args:
        plan_id: The ID of the plan. (e.g. plans/asfoghaev)
        plan_title: The title of the plan.
    """

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call with the `plan_id`."""
        self._url = f"{CIRCUIT_URL}/{self._plan_id}:optimize"


class OptimizationChecker(BaseOptimizationCaller, BaseGetCaller):
    """A class for checking the status of an optimization."""

    _timeout: float = RateLimits.READ_TIMEOUT_SECONDS
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
        self._url = f"{CIRCUIT_URL}/{self.operation_id}"


class PlanDistributor(BasePostCaller):
    """Class for distributing plans."""

    distributed: bool

    #: The ID of the plan.
    _plan_id: str
    #: The title of the plan
    _plan_title: str

    @typechecked
    def __init__(self, plan_id: str, plan_title: str) -> None:
        """Initialize the PlanDistributor object.

        Args:
            plan_id: The ID of the plan. (e.g. plans/asfoghaev)
            plan_title: The title of the plan.
        """
        self._plan_id = plan_id
        self._plan_title = plan_title
        super().__init__()

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call with the `plan_id`."""
        self._url = f"{CIRCUIT_URL}/{self._plan_id}:distribute"

    @typechecked
    def _handle_200(self) -> None:
        """Handle a 200 response.

        Raises:
            RuntimeError: If the plan was not distributed.
        """
        super()._handle_200()
        self.distributed = self.response_json[CircuitColumns.DISTRIBUTED]
        if not self.distributed:
            raise RuntimeError(
                f"Failed to distribute plan {self._plan_title} ({self._plan_id}):"
                f"\n{self.response_json}"
            )


class PlanDeleter(BaseDeleteCaller):
    """Class for deleting plans."""

    #: Whether the plan was deleted.
    deletion: bool = False

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
        """Set the URL for the API call with the `plan_id`."""
        self._url = f"{CIRCUIT_URL}/{self._plan_id}"

    @typechecked
    def _handle_204(self) -> None:
        """Handle a 204 response.

        Sets `deletion` to True.
        """
        super()._handle_204()
        self.deletion = True
