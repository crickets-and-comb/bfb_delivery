"""Classes for making API calls."""

import logging

from typeguard import typechecked

from comb_utils import (
    BaseCaller,
    BaseDeleteCaller,
    BaseGetCaller,
    BasePagedResponseGetter,
    BasePostCaller,
)

from bfb_delivery.lib.constants import CIRCUIT_URL, CircuitColumns, RateLimits
from bfb_delivery.lib.dispatch.utils import get_circuit_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# TODO: https://github.com/crickets-and-comb/bfb_delivery/issues/138:
# Why are we using _set_url instead of the url property?
# Why are we using _set_request_call instead of the _request_call property?


class BaseKeyRetriever:
    """A base class for getting the API key.

    Presets the API key to be used for authentication.
    """

    @typechecked
    def _get_API_key(self) -> str:
        """Get the API key.

        Returns:
            The API key.
        """
        return get_circuit_key()


class BaseBFBGetCaller(BaseKeyRetriever, BaseGetCaller):
    """A base class for making GET API calls with BFB Circuit key."""


class BaseBFBPostCaller(BaseKeyRetriever, BasePostCaller):
    """A base class for making POST API calls with BFB Circuit key."""


class BaseBFBDeleteCaller(BaseKeyRetriever, BaseDeleteCaller):
    """A base class for making DELETE API calls with BFB Circuit key."""


class BaseOptimizationCaller(BaseKeyRetriever, BaseCaller):
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


class PagedResponseGetterBFB(BaseKeyRetriever, BasePagedResponseGetter):
    """Class for getting paged responses."""


class PlanInitializer(BaseBFBPostCaller):
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


class StopUploader(BaseBFBPostCaller):
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


class OptimizationLauncher(BaseOptimizationCaller, BaseBFBPostCaller):
    """A class for launching route optimization.

    Args:
        plan_id: The ID of the plan. (e.g. plans/asfoghaev)
        plan_title: The title of the plan.
    """

    @typechecked
    def _set_url(self) -> None:
        """Set the URL for the API call with the `plan_id`."""
        self._url = f"{CIRCUIT_URL}/{self._plan_id}:optimize"


class OptimizationChecker(BaseOptimizationCaller, BaseBFBGetCaller):
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


class PlanDistributor(BaseBFBPostCaller):
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


class PlanDeleter(BaseBFBDeleteCaller):
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
