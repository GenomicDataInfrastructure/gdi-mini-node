import logging

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from ..model.common import BeaconError, BeaconResponse, BeaconRequest, \
    RequestMeta, BeaconQuery
from ..setup import BeaconSetup

_log = logging.getLogger(__name__)


class BeaconErrorResponseHandler:
    """API middleware for capturing different errors and returning BeaconError
    responses.
    """

    def __init__(
            self,
            aggregated_setup: BeaconSetup | None,
            sensitive_setup: BeaconSetup | None,
    ) -> None:
        """Beacon setup instances are used to determine which beacon was
        targeted with the request, and also for adapting the response format
        according to its configuration values.

        They are optional as either or both of them can be disabled by omitting
        their configuration files.

        Args:
            aggregated_setup: Beacon setup for the aggregated Beacon, if
              available.
            sensitive_setup: Beacon setup for the sensitive Beacon, if
              available.
        """
        self._aggregated_setup = aggregated_setup
        self._sensitive_setup = sensitive_setup

    def on_validation_error(
            self, request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Renders an error response for validation errors (typically issues
        with the request JSON payload).
        """
        message = "\n".join(
            [
                f"{detail.get('msg', 'no msg')}: {detail.get('loc', 'no loc')}"
                for detail in exc.errors()
            ]
        )

        return self._create_error_response(
            request,
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            message,
        )

    def on_http_error(self, request: Request, e: HTTPException) -> JSONResponse:
        """Renders an error response for HTTP request issues."""
        return self._create_error_response(request, e.status_code, e.detail)

    def on_system_error(self, request: Request, exc: Exception) -> JSONResponse:
        """Renders an error response for internal errors, where the details are
        not exposed publicly.
        """
        _log.error(
            "Detected an error while handling request [%s %s?%s]",
            request.method,
            request.url.path,
            request.query_params,
            exc_info=exc,
        )

        return self._create_error_response(
            request,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Failed to serve the request due to technical error",
        )

    def _create_error_response(
            self, request: Request, error_code: int, error_message: str
    ) -> JSONResponse:
        """Composes the final JSON for the error response.

        Response data depends on which Beacon was the receiver of the request.
        If none of them, a way simpler error JSON is returned.

        This method also relies on input JSON from the payload. Endpoints need
        to store it in request state (request.state.BeaconRequest). If it's
        missing, an empty request state will be used.
        """
        try:
            beacon_request: BeaconRequest = request.state.BeaconRequest
        except AttributeError:
            beacon_request = BeaconRequest(
                meta=RequestMeta(apiVersion=""),
                query=BeaconQuery(),
            )

        setup = None
        for item in [self._aggregated_setup, self._sensitive_setup]:
            if item is not None and request.url.path.startswith(item.base_path):
                setup = item

        # Simple JSON error-response for non-Beacon paths:
        if setup is None:
            return _default_error_response(error_code, error_message)

        # Constructing Beacon error:
        meta = setup.query_response_meta(beacon_request, None)
        error_info = BeaconError(
            errorCode=error_code,
            errorMessage=error_message,
        )
        response = BeaconResponse(meta=meta, error=error_info)

        return JSONResponse(
            status_code=error_code,
            content=response.model_dump(exclude_none=True),
        )


def _default_error_response(error_code: int, error_message: str):
    return JSONResponse(
        status_code=error_code,
        content={
            "status_code": error_code,
            "message": error_message,
        },
    )
