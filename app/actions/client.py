"""Typed client for the prediction and per-result feature APIs on Studio develop."""

import json
import logging
from datetime import datetime
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

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

# The provider's feature page defaults to 50 if the caller says nothing, which
# is small enough to turn one poll into hundreds of round trips. Always send a
# size; these are what we send when the config does not override them.
DEFAULT_FEATURE_PAGE_SIZE = 500
DEFAULT_PREDICTION_PAGE_SIZE = 100


class _Filter(pydantic.BaseModel):
    """Base for the operator objects. Unset operators are never serialized."""

    class Config:
        extra = "forbid"


class KeywordFilter(_Filter):
    """Exact-match filter over an identifier-like field."""

    eq: Optional[str] = None
    neq: Optional[str] = None
    inc: Optional[List[str]] = None
    ninc: Optional[List[str]] = None
    exists: Optional[bool] = None


class StringFilter(_Filter):
    """Keyword operators plus SQL-style `like` matching."""

    eq: Optional[str] = None
    neq: Optional[str] = None
    inc: Optional[List[str]] = None
    ninc: Optional[List[str]] = None
    like: Optional[str] = None
    nlike: Optional[str] = None


class NumericFilter(_Filter):
    eq: Optional[float] = None
    neq: Optional[float] = None
    gte: Optional[float] = None
    gt: Optional[float] = None
    lte: Optional[float] = None
    lt: Optional[float] = None


class DatetimeFilter(_Filter):
    gte: Optional[datetime] = None
    gt: Optional[datetime] = None
    lte: Optional[datetime] = None
    lt: Optional[datetime] = None
    exists: Optional[bool] = None


class PropertyFilter(pydantic.BaseModel):
    """A filter on one arbitrary feature property.

    The model that produced the features decides what properties exist, so
    these are the escape hatch for model-specific thresholds — the common case
    being a confidence score: ``{"property_name": "confidence",
    "numeric_filter": {"gte": 0.8}}``.
    """

    property_name: str
    keyword_filter: Optional[KeywordFilter] = None
    string_filter: Optional[StringFilter] = None
    numeric_filter: Optional[NumericFilter] = None
    datetime_filter: Optional[DatetimeFilter] = None


class Geometry(pydantic.BaseModel):
    """A GeoJSON geometry, kept loose on purpose.

    `coordinates` is nested to a depth that depends on `type`, so it is typed
    as Any rather than reproducing the GeoJSON grammar. Anything we send came
    from an operator's AOI; anything we receive came from the API.
    """

    type: str
    coordinates: Any = None
    bbox: Optional[List[float]] = None
    # A GeometryCollection carries `geometries` instead of `coordinates`.
    geometries: Optional[List[Dict[str, Any]]] = None

    class Config:
        extra = "allow"


# --------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------
class _SearchRequest(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    def as_body(self) -> dict:
        """The request body with every unset operator dropped.

        Goes through ``json()`` rather than ``dict()`` so datetimes are ISO
        strings and the body is JSON-serializable as-is.
        """
        return json.loads(self.json(exclude_none=True))


class PredictionSearchRequest(_SearchRequest):
    sort_by: str = "creation_time"
    sort_direction: str = "desc"
    limit: int = DEFAULT_PREDICTION_PAGE_SIZE
    offset: int = 0
    id: Optional[KeywordFilter] = None
    organization_id: Optional[KeywordFilter] = None
    project_id: Optional[KeywordFilter] = None
    model_id: Optional[KeywordFilter] = None
    requester_id: Optional[KeywordFilter] = None
    name: Optional[StringFilter] = None
    status: Optional[KeywordFilter] = None
    workflow_type: Optional[KeywordFilter] = None
    creation_time: Optional[DatetimeFilter] = None
    deleted_time: Optional[DatetimeFilter] = None


class FeatureSearchRequest(_SearchRequest):
    limit: int = DEFAULT_FEATURE_PAGE_SIZE
    offset: int = 0
    sort_by: str = "oe_created_at"
    sort_direction: str = "asc"
    id: Optional[KeywordFilter] = None
    intersects_geometry: Optional[Geometry] = None
    oe_prediction_result_file_id: Optional[KeywordFilter] = None
    oe_start_time: Optional[DatetimeFilter] = None
    oe_end_time: Optional[DatetimeFilter] = None
    oe_created_at: Optional[DatetimeFilter] = None
    property_filters: Optional[List[PropertyFilter]] = None


# --------------------------------------------------------------------------
# Responses
# --------------------------------------------------------------------------
class SearchMeta(pydantic.BaseModel):
    total: Optional[int] = None

    class Config:
        extra = "allow"


class ApiError(pydantic.BaseModel):
    code: Optional[str] = None
    message: Optional[str] = None

    class Config:
        extra = "allow"


class PredictionResult(pydantic.BaseModel):
    id: str
    file_formats: List[str] = pydantic.Field(default_factory=list)


class Prediction(pydantic.BaseModel):
    """One prediction run, as the dropdown and the auth check see it."""

    id: str
    name: Optional[str] = None
    status: Optional[str] = None
    model_id: Optional[str] = None
    project_id: Optional[str] = None
    organization_id: Optional[str] = None
    requester_id: Optional[str] = None
    workflow_type: Optional[str] = None
    creation_time: Optional[datetime] = None
    deleted_time: Optional[datetime] = None
    result: Optional[PredictionResult] = None
    error_message: Optional[str] = None

    class Config:
        extra = "allow"


class FeatureProperties(pydantic.BaseModel):
    """The `oe_`-prefixed properties every feature carries, plus the model's own.

    The model-authored properties are the point of the record and vary per
    model, so they arrive as extras and are read back with `model_properties()`.
    """

    oe_start_time: Optional[datetime] = None
    oe_end_time: Optional[datetime] = None
    oe_created_at: Optional[datetime] = None
    oe_prediction_result_id: Optional[str] = None
    oe_prediction_result_file_id: Optional[str] = None

    class Config:
        extra = "allow"

    def model_properties(self) -> Dict[str, Any]:
        """Everything the model itself put on the feature, without the `oe_`
        system fields — those are provenance, and the handler reports them
        separately rather than burying them among the model's own outputs."""
        return {
            name: value
            for name, value in self.dict(exclude_none=True).items()
            if not name.startswith("oe_")
        }


class Feature(pydantic.BaseModel):
    """A GeoJSON Feature as returned by the features search.

    `id` is normalized to a string: the documented response returns an integer
    (`"id": 1`) while the documented request filters the same field as a
    keyword (`{"eq": ""}`). Picking one here means a feature's identity does not
    change type depending on which end of the API it came from — which matters,
    because it ends up in the event's provenance identity and in the state
    row that stops a detection being ingested twice.
    """

    id: Optional[str] = None
    type: str = "Feature"
    geometry: Optional[Geometry] = None
    bbox: Optional[List[float]] = None
    properties: FeatureProperties = pydantic.Field(default_factory=FeatureProperties)

    class Config:
        extra = "allow"


class PredictionSearchResponse(pydantic.BaseModel):
    records: List[Prediction] = pydantic.Field(default_factory=list)
    meta: SearchMeta = pydantic.Field(default_factory=SearchMeta)
    errors: List[ApiError] = pydantic.Field(default_factory=list)

    class Config:
        extra = "allow"


class FeatureSearchResponse(pydantic.BaseModel):
    records: List[Feature] = pydantic.Field(default_factory=list)
    meta: SearchMeta = pydantic.Field(default_factory=SearchMeta)
    errors: List[ApiError] = pydantic.Field(default_factory=list)

    class Config:
        extra = "allow"


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
        self._api_token = api_token
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
    async def _post(self, path: str, body: dict) -> dict:
        return await self._request("POST", path, body)

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
        payload = await self._post(self.PREDICTIONS_PATH, request.as_body())
        return self._parse(PredictionSearchResponse, payload, self.PREDICTIONS_PATH)

    async def get_prediction(self, prediction_id: str) -> Prediction:
        path = f"/api/v1/predictions/{prediction_id}"
        return self._one_prediction(await self._request("GET", path), path)

    async def create_prediction(self, body: dict) -> Prediction:
        path = "/api/v1/predictions"
        # Studio has no idempotency key: an ambiguous POST must be reconciled, never retried.
        return self._one_prediction(
            await self._request("POST", path, body, attempts=1), path
        )

    def _one_prediction(self, payload: dict, path: str) -> Prediction:
        response = self._parse(PredictionSearchResponse, payload, path)
        if len(response.records) != 1:
            raise IntegrationBadResponseError(f"Expected one prediction from {path}.")
        return response.records[0]

    async def project_organization(self, project_id: str) -> str:
        path = f"/api/v1/projects/{project_id}"
        payload = await self._request("GET", path)
        records = payload.get("records", [])
        if (
            payload.get("errors")
            or len(records) != 1
            or not records[0].get("organization_id")
        ):
            raise IntegrationBadResponseError(
                "Project has no organization or returned an incomplete response."
            )
        return records[0]["organization_id"]

    async def search_features(
        self, result_id: str, request: FeatureSearchRequest
    ) -> FeatureSearchResponse:
        path = self.FEATURES_PATH.format(result_id=result_id)
        payload = await self._post(path, request.as_body())
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
    if isinstance(exc, (IntegrationConnectionError, IntegrationRateLimitError)):
        return True
    if isinstance(exc, IntegrationBadResponseError):
        return (source_status_code(exc) or 0) >= 500
    return False


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
