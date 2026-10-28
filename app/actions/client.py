"""Typed client for the OlmoEarth prediction and per-result feature APIs (Studio develop).

Only the request filters and response fields the monitor uses are modelled; the
API accepts and returns more.
"""

import json
from typing import Any, Dict, List, Optional

import httpx
import pydantic
import stamina

from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
    IntegrationRateLimitError,
    source_status_code,
)

DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


class _Filter(pydantic.BaseModel):
    """Base for the operator objects. Unset operators are never serialized."""

    class Config:
        extra = "forbid"


class KeywordFilter(_Filter):
    eq: Optional[str] = None
    inc: Optional[List[str]] = None


class StringFilter(_Filter):
    eq: Optional[str] = None


class PropertyFilter(pydantic.BaseModel):
    """A filter on one model-authored feature property."""

    property_name: str
    keyword_filter: KeywordFilter


class Geometry(pydantic.BaseModel):
    """A GeoJSON geometry, kept loose: ``coordinates`` nests by ``type``."""

    type: str
    coordinates: Any = None

    class Config:
        extra = "allow"


# --------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------
class _SearchRequest(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    def as_body(self) -> dict:
        """The request body with every unset field dropped, JSON-serializable as-is."""
        return json.loads(self.json(exclude_none=True))


class PredictionSearchRequest(_SearchRequest):
    limit: int = 100
    project_id: Optional[KeywordFilter] = None
    model_id: Optional[KeywordFilter] = None
    name: Optional[StringFilter] = None


class FeatureSearchRequest(_SearchRequest):
    # The API defaults to 50 per page; always send a size.
    limit: int
    offset: int = 0
    sort_by: str = "oe_created_at"
    sort_direction: str = "asc"
    intersects_geometry: Optional[Geometry] = None
    property_filters: List[PropertyFilter] = pydantic.Field(default_factory=list)


# --------------------------------------------------------------------------
# Responses
# --------------------------------------------------------------------------
class SearchMeta(pydantic.BaseModel):
    total: Optional[int] = None

    class Config:
        extra = "allow"


class _Records(pydantic.BaseModel):
    """The ``records``/``errors`` envelope every endpoint returns."""

    meta: SearchMeta = pydantic.Field(default_factory=SearchMeta)
    errors: List[Any] = pydantic.Field(default_factory=list)

    class Config:
        extra = "allow"


class PredictionResult(pydantic.BaseModel):
    id: str
    file_formats: List[str] = pydantic.Field(default_factory=list)


class Prediction(pydantic.BaseModel):
    id: str
    name: Optional[str] = None
    status: Optional[str] = None
    result: Optional[PredictionResult] = None
    error_message: Optional[str] = None

    class Config:
        extra = "allow"


class PredictionSearchResponse(_Records):
    records: List[Prediction] = pydantic.Field(default_factory=list)


class FeatureProperties(pydantic.BaseModel):
    """The ``oe_`` system properties every feature carries, plus the model's own."""

    oe_prediction_result_id: Optional[str] = None

    class Config:
        extra = "allow"

    def model_properties(self) -> Dict[str, Any]:
        """Everything the model put on the feature, without the ``oe_`` provenance fields."""
        return {
            name: value
            for name, value in self.dict(exclude_none=True).items()
            if not name.startswith("oe_")
        }


class Feature(pydantic.BaseModel):
    """A GeoJSON Feature from the features search.

    ``id`` is normalized to a string: the API returns integers here but filters
    the same field as a keyword, and the ID feeds each event's stable identity.
    """

    id: Optional[str] = None
    geometry: Optional[Geometry] = None
    properties: FeatureProperties = pydantic.Field(default_factory=FeatureProperties)

    class Config:
        extra = "allow"


class FeatureSearchResponse(_Records):
    records: List[Feature] = pydantic.Field(default_factory=list)


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------
class OlmoEarthClient:
    """Async transport for prediction lifecycle and feature searches.

    Use as an async context manager so the underlying connection pool is closed
    even when an action raises partway through paging.
    """

    FEATURES_PATH = "/api/v1/prediction-results/{result_id}/features/search"
    PREDICTIONS_PATH = "/api/v1/predictions/search"

    def __init__(
        self,
        base_url: str,
        api_token: str,
        timeout: httpx.Timeout = DEFAULT_TIMEOUT,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        if not base_url:
            raise IntegrationConfigurationError(
                "No base URL is set for this integration; set the site URL to the "
                "OlmoEarth API root."
            )
        if not api_token:
            raise IntegrationConfigurationError(
                "No API token is configured; run the Authenticate action first."
            )
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {api_token}",
            },
            # Tests inject an httpx.MockTransport here; production passes none
            # and httpx builds its own.
            transport=transport,
        )

    async def __aenter__(self) -> "OlmoEarthClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    # -- transport ---------------------------------------------------------
    async def _request(
        self, method: str, path: str, body: Optional[dict] = None, attempts: int = 3
    ) -> dict:
        async for attempt in stamina.retry_context(
            on=_is_transient,
            attempts=attempts,
            wait_initial=2.0,
            wait_max=20.0,
            wait_jitter=2.0,
        ):
            with attempt:
                try:
                    response = await self._client.request(method, path, json=body)
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    raise _classify_status_error(e) from e
                except httpx.HTTPError as e:
                    raise IntegrationConnectionError(
                        f"Could not reach the OlmoEarth API at {self.base_url}: {e}"
                    ) from e
        try:
            return response.json()
        except ValueError as e:
            raise IntegrationBadResponseError(
                f"OlmoEarth returned a non-JSON response from {path}."
            ) from e

    @staticmethod
    def _parse(model, payload: dict, path: str):
        try:
            parsed = model.parse_obj(payload)
        except pydantic.ValidationError as e:
            raise IntegrationBadResponseError(
                f"OlmoEarth returned an unexpected response shape from {path}: {e}"
            ) from e
        if parsed.errors:
            raise IntegrationBadResponseError(
                f"OlmoEarth returned incomplete results from {path}."
            )
        return parsed

    # -- endpoints ---------------------------------------------------------
    async def search_predictions(
        self, request: PredictionSearchRequest
    ) -> PredictionSearchResponse:
        payload = await self._request(
            "POST", self.PREDICTIONS_PATH, request.as_body()
        )
        return self._parse(PredictionSearchResponse, payload, self.PREDICTIONS_PATH)

    async def get_prediction(self, prediction_id: str) -> Prediction:
        path = f"/api/v1/predictions/{prediction_id}"
        return self._one(PredictionSearchResponse, await self._request("GET", path), path)

    async def create_prediction(self, body: dict) -> Prediction:
        path = "/api/v1/predictions"
        # Studio has no idempotency key: never retry the POST. The monitor looks the
        # prediction up by its unique name before posting again.
        return self._one(
            PredictionSearchResponse,
            await self._request("POST", path, body, attempts=1),
            path,
        )

    def _one(self, model, payload: dict, path: str):
        response = self._parse(model, payload, path)
        if len(response.records) != 1:
            raise IntegrationBadResponseError(f"Expected exactly one record from {path}.")
        return response.records[0]

    async def search_features(
        self, result_id: str, request: FeatureSearchRequest
    ) -> FeatureSearchResponse:
        path = self.FEATURES_PATH.format(result_id=result_id)
        payload = await self._request("POST", path, request.as_body())
        return self._parse(FeatureSearchResponse, payload, path)

    async def verify_token(self) -> Optional[int]:
        """Prove the token authenticates; report how many predictions it can see.

        Asks the predictions search rather than the features search, and that
        choice is the whole point of the method: the features endpoint is
        `optional_auth`, so an invalid or expired token is not rejected there —
        the caller is quietly downgraded to anonymous and gets the public
        Results. The predictions search requires a real user, so it is the one
        that gives a straight answer. Callers need the answer, not the reason.
        """
        response = await self.search_predictions(PredictionSearchRequest(limit=1))
        return response.meta.total


def _is_transient(exc: BaseException) -> bool:
    """Retry predicate: transport failures, 429 and 5xx are worth another try."""
    if isinstance(exc, IntegrationConnectionError):
        return True
    status = source_status_code(exc)
    return status is not None and (status == 429 or status >= 500)


def _classify_status_error(exc: httpx.HTTPStatusError):
    """Turn an HTTP status into the runner's classified error for that status."""
    status = exc.response.status_code
    detail = _response_detail(exc.response)
    if status in (401, 403):
        return IntegrationAuthError(
            f"OlmoEarth rejected the API token ({status}). {detail}".strip(),
            status_code=status,
        )
    if status == 429:
        return IntegrationRateLimitError(
            f"OlmoEarth rate-limited the request. {detail}".strip(), status_code=status
        )
    if status == 404:
        return IntegrationBadResponseError(
            f"OlmoEarth has no record at {exc.request.url.path}. {detail}".strip(),
            status_code=status,
        )
    return IntegrationBadResponseError(
        f"OlmoEarth returned {status} for {exc.request.url.path}. {detail}".strip(),
        status_code=status,
    )


def _response_detail(response: httpx.Response, limit: int = 500) -> str:
    """A short, safe excerpt of an error body for the activity log."""
    try:
        text = response.text or ""
    except Exception:  # a streamed or already-closed body
        return ""
    text = " ".join(text.split())
    return text[:limit] + ("…" if len(text) > limit else "")
