"""HTTP client for the OlmoEarth prediction API.

Two endpoints carry this integration:

  POST /api/v1/prediction-results/features/search
      The ingest. Returns GeoJSON detections from every Prediction Result the
      token can read, in one request. The body has two halves: ``prediction_
      results`` picks *which* results to read (by model, project, area, time)
      and is where the server enforces access control; ``features`` filters the
      detections within them (geometry, observation window, arbitrary
      properties) and carries the sort and paging.

  POST /api/v1/predictions/search
      Prediction runs — what the portal dropdown lists, and the request the
      auth action uses to prove a token works.

Every filter the API accepts is an object of comparison operators
(``{"eq": ...}``, ``{"gte": ...}``) and every operator is optional. The models
below mirror that shape, and requests are serialized with ``exclude_none=True``
so an operator we did not set is absent from the body rather than sent as an
empty string: the documented sample shows ``{"eq": ""}``, but sent literally
that asks for records whose field equals the empty string, which matches
nothing.

The features endpoint is `optional_auth` on the provider's side: an invalid or
expired token does not 401, it degrades the caller to anonymous and returns
only Results marked public. Nothing here can tell that apart from "no new
detections", so the pull action confirms the token against the predictions
search — which does require auth — before reporting an empty run.
"""
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx
import pydantic
import stamina

from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
    IntegrationRateLimitError,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

# The provider's feature page defaults to 50 if the caller says nothing, which
# is small enough to turn one poll into hundreds of round trips. Always send a
# size; these are what we send when the config does not override them.
DEFAULT_FEATURE_PAGE_SIZE = 500
DEFAULT_PREDICTION_PAGE_SIZE = 100

# A runaway query (an AOI covering a continent, a model that emits millions of
# detections) would otherwise page forever and blow the action's memory. Every
# paging helper stops here and says so.
MAX_PAGES = 200


# --------------------------------------------------------------------------
# Filter primitives
# --------------------------------------------------------------------------
class _Filter(pydantic.BaseModel):
    """Base for the operator objects. Unset operators are never serialized."""

    class Config:
        extra = "forbid"

    def is_empty(self) -> bool:
        return not self.dict(exclude_none=True)


class KeywordFilter(_Filter):
    """Exact-match filter over an identifier-like field."""

    eq: Optional[str] = None
    neq: Optional[str] = None
    inc: Optional[List[str]] = None
    ninc: Optional[List[str]] = None
    exists: Optional[bool] = None


class OrganizationFilter(_Filter):
    """Organization accepts only `eq` and `inc` (see the documented schema)."""

    eq: Optional[str] = None
    inc: Optional[List[str]] = None


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
        json_encoders = {datetime: lambda dt: dt.isoformat()}

    def as_body(self) -> dict:
        """The request body with every unset operator dropped.

        Goes through ``json()`` rather than ``dict()`` so datetimes are ISO
        strings and the body is JSON-serializable as-is.
        """
        import json

        return json.loads(self.json(exclude_none=True, exclude_defaults=False))


class PredictionSearchRequest(_SearchRequest):
    sort_by: str = "creation_time"
    sort_direction: str = "desc"
    limit: int = DEFAULT_PREDICTION_PAGE_SIZE
    offset: int = 0
    id: Optional[KeywordFilter] = None
    organization_id: Optional[OrganizationFilter] = None
    project_id: Optional[KeywordFilter] = None
    model_id: Optional[KeywordFilter] = None
    requester_id: Optional[KeywordFilter] = None
    name: Optional[StringFilter] = None
    status: Optional[KeywordFilter] = None
    workflow_type: Optional[KeywordFilter] = None
    creation_time: Optional[DatetimeFilter] = None
    start_time: Optional[DatetimeFilter] = None
    end_time: Optional[DatetimeFilter] = None
    target_area_id: Optional[KeywordFilter] = None
    intersects_geometry: Optional[Geometry] = None
    deleted_time: Optional[DatetimeFilter] = None


class PredictionResultFilters(_SearchRequest):
    """The `prediction_results` half: which Results the feature search reads.

    This is the access-controlled half — the server turns these filters into
    the set of Result ids the token may read and scopes the feature query to
    it. An empty object is legal and means "everything this token can see",
    which for a service user in a single-project organization is a reasonable
    feed and for anything wider is a mistake; `PullEventsConfig` requires at
    least one scope rather than relying on that.

    `prediction_deleted_time` is deliberately absent: the provider defaults it
    to `exists=false`, which excludes the Results of soft-deleted Predictions,
    and that is the behaviour an ingest wants.
    """

    id: Optional[KeywordFilter] = None
    organization_id: Optional[KeywordFilter] = None
    access_level: Optional[KeywordFilter] = None
    creation_time: Optional[DatetimeFilter] = None
    updated_time: Optional[DatetimeFilter] = None
    prediction_model_id: Optional[KeywordFilter] = None
    prediction_project_id: Optional[KeywordFilter] = None
    prediction_target_area_id: Optional[KeywordFilter] = None
    prediction_start_time: Optional[DatetimeFilter] = None
    prediction_end_time: Optional[DatetimeFilter] = None
    prediction_intersects_geometry: Optional[Geometry] = None


class FeatureFilters(_SearchRequest):
    """The `features` half: which detections to return from those Results.

    `oe_prediction_result_id` is *not* a field here, and its absence is the
    point. The server derives that filter from `prediction_results` — that
    derivation is the access control — and rejects a caller-supplied value with
    a 422 rather than honouring or silently overwriting it. Leaving it off the
    model means this connector cannot send one by accident.
    """

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


class FeatureSearchRequest(_SearchRequest):
    """The whole body of a cross-Result feature search."""

    prediction_results: PredictionResultFilters = pydantic.Field(
        default_factory=PredictionResultFilters
    )
    features: FeatureFilters = pydantic.Field(default_factory=FeatureFilters)


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
    because it ends up in the event's `external_source_id` and in the state
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
    """Thin async wrapper over the two search endpoints.

    Use as an async context manager so the underlying connection pool is closed
    even when an action raises partway through paging.
    """

    FEATURES_PATH = "/api/v1/prediction-results/features/search"
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
        """POST a search body and return the decoded response.

        Retries transport failures, 429 and 5xx; a 4xx is a definite answer and
        fails immediately. Failures come back as `IntegrationError` subclasses
        so the runner records a classified status on the integration rather
        than an opaque traceback.
        """
        async for attempt in stamina.retry_context(
            on=_is_transient,
            attempts=3,
            wait_initial=2.0,
            wait_max=20.0,
            wait_jitter=2.0,
        ):
            with attempt:
                try:
                    response = await self._client.post(path, json=body)
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
        # A 200 can still carry errors alongside records. Records present means
        # a partial result worth keeping; an error with nothing to show for it
        # is a failure.
        if parsed.errors and not parsed.records:
            detail = "; ".join(
                filter(None, (f"{e.code or 'error'}: {e.message or ''}".strip(" :") for e in parsed.errors))
            )
            raise IntegrationBadResponseError(f"OlmoEarth rejected the search: {detail}")
        if parsed.errors:
            logger.warning(
                "OlmoEarth returned %s error(s) alongside %s record(s) from %s: %s",
                len(parsed.errors), len(parsed.records), path,
                [e.dict(exclude_none=True) for e in parsed.errors],
            )
        return parsed

    # -- endpoints ---------------------------------------------------------
    async def search_predictions(self, request: PredictionSearchRequest) -> PredictionSearchResponse:
        payload = await self._post(self.PREDICTIONS_PATH, request.as_body())
        return self._parse(PredictionSearchResponse, payload, self.PREDICTIONS_PATH)

    async def search_features(self, request: FeatureSearchRequest) -> FeatureSearchResponse:
        payload = await self._post(self.FEATURES_PATH, request.as_body())
        return self._parse(FeatureSearchResponse, payload, self.FEATURES_PATH)

    # -- paging ------------------------------------------------------------
    async def iter_features(self, request: FeatureSearchRequest) -> AsyncIterator[Feature]:
        """Yield every feature matching `request`, a page at a time.

        Paging is by offset, so it forces ascending order on `oe_created_at`:
        with a descending sort a feature written between two requests shifts
        every later record one slot forward and the walk skips one. Ascending
        order only ever appends beyond the window already read.

        There is no feature cap here. The caller stops when it has taken enough
        *new* detections, which is not the same count: every run re-reads the
        watermark second, and charging those re-reads against the cap is what
        would let a crowded second consume a whole run without progress.
        """
        request = request.copy(deep=True)
        request.features.sort_by = "oe_created_at"
        request.features.sort_direction = "asc"
        request.features.offset = request.features.offset or 0
        yielded = 0
        for _ in range(MAX_PAGES):
            response = await self.search_features(request)
            if not response.records:
                return
            for feature in response.records:
                yield feature
                yielded += 1
            # A short page is the last page. `meta.total` is a second, cheaper
            # stop for a provider that always fills the page.
            if len(response.records) < request.features.limit:
                return
            request.features.offset += request.features.limit
            if response.meta.total is not None and request.features.offset >= response.meta.total:
                return
        logger.warning(
            "Stopped paging features after %s pages (%s features). Narrow the query "
            "— by area, model, time window or a property filter — to see the rest.",
            MAX_PAGES, yielded,
        )


def _is_transient(exc: BaseException) -> bool:
    """Retry predicate: transport failures, 429 and 5xx are worth another try."""
    if isinstance(exc, (IntegrationConnectionError, IntegrationRateLimitError)):
        return True
    if isinstance(exc, IntegrationBadResponseError):
        return (getattr(exc, "status_code", None) or 0) >= 500
    return False


def _classify_status_error(exc: httpx.HTTPStatusError):
    """Turn an HTTP status into the runner's classified error for that status."""
    status = exc.response.status_code
    detail = _response_detail(exc.response)
    if status in (401, 403):
        return IntegrationAuthError(
            f"OlmoEarth rejected the API token ({status}). {detail}".strip(), status_code=status
        )
    if status == 429:
        return IntegrationRateLimitError(
            f"OlmoEarth rate-limited the request. {detail}".strip(), status_code=status
        )
    if status == 400 and "Prediction Results" in detail:
        # The provider caps how many Prediction Results one feature search may
        # span and refuses rather than truncating, because a partial feature
        # set is indistinguishable from a complete one. That is a scope this
        # integration chose, so say which knobs narrow it.
        #
        # The provider's own wording stays in the log, not in the message:
        # IntegrationConfigurationError is the one connector error forwarded
        # verbatim to the ephemeral caller, and its contract is to describe the
        # shape of the problem without echoing values back.
        logger.warning("OlmoEarth refused the search as too broad: %s", detail)
        return IntegrationConfigurationError(
            "This integration's filters match more Prediction Results than "
            "OlmoEarth will search at once. Narrow it with a target area, a "
            "model, a project, or a shorter lookback.",
            status_code=status,
        )
    if status == 404:
        return IntegrationBadResponseError(
            f"OlmoEarth has no record at {exc.request.url.path}. {detail}".strip(), status_code=status
        )
    return IntegrationBadResponseError(
        f"OlmoEarth returned {status} for {exc.request.url.path}. {detail}".strip(), status_code=status
    )


def _response_detail(response: httpx.Response, limit: int = 500) -> str:
    """A short, safe excerpt of an error body for the activity log."""
    try:
        text = response.text or ""
    except Exception:  # a streamed or already-closed body
        return ""
    text = " ".join(text.split())
    return text[:limit] + ("…" if len(text) > limit else "")
