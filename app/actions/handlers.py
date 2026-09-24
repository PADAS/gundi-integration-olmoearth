"""Action handlers for the OlmoEarth integration.

The ingest is one request deep. OlmoEarth resolves which Prediction Results the
token may read from the `prediction_results` filters and queries the feature
index once, so a run is:

    prediction-results/features/search   -> GeoJSON detections, paged
      -> Gundi events                    -> one event per feature

The watermark is `oe_created_at` on the features themselves, carried in the
response of that same request. Two details keep it from dropping detections:

  * The filter is `gte`, not `gt`. A bulk-inserted Result gives thousands of
    features an identical `oe_created_at`; if some of them are indexed after a
    run has already read that second and moved past it, `gt` would never see
    them again. `gte` re-reads the boundary second every run.
  * `boundary_feature_ids` is what stops that re-read becoming duplicate
    events: the ids of the features already ingested at exactly the watermark
    second. It is bounded, and overflowing it re-sends detections rather than
    losing them.

Paging forces `sort_by=oe_created_at, sort_direction=asc` regardless of what
the caller asked for, because offset paging over a descending sort skips a
record whenever one is written mid-walk.
"""
import datetime
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pydantic

import app.actions.client as client
import app.services.gundi as gundi_tools
from app.actions.core import ReferenceDataResponse, ReferenceOption, action_title
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.errors import IntegrationConfigurationError
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action

from .configurations import (
    AuthenticateConfig,
    ListPredictionsConfig,
    PullEventsConfig,
)

logger = logging.getLogger(__name__)

state_manager = IntegrationStateManager()

PULL_EVENTS_ACTION_ID = "pull_events"

# How many detection identities to remember at the watermark second. The
# feature search is bounded with `oe_created_at >= watermark` (not `>`), so
# everything sharing that second comes back every run; this set is what
# recognises it. Bounded so the state row cannot grow without limit — and when
# a single second holds more detections than this, the overflow is re-sent
# rather than dropped.
#
# No constant can make that overflow impossible, because the provider decides
# how many detections share a second. What keeps the ingest moving is not this
# number but the rule in the run loop below: a run never stops inside a second.
BOUNDARY_ID_MEMORY = 5_000


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
def get_auth_config(integration) -> AuthenticateConfig:
    auth_config = find_config_for_action(integration.configurations, "auth")
    if not auth_config:
        raise IntegrationConfigurationError(
            "This integration has no authentication configured. Add the OlmoEarth "
            "API token in the Authenticate action."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def client_for(integration, auth_config: Optional[AuthenticateConfig] = None) -> client.OlmoEarthClient:
    """An API client bound to this integration's site URL and token."""
    auth_config = auth_config or get_auth_config(integration)
    return client.OlmoEarthClient(
        base_url=integration.base_url,
        api_token=auth_config.api_token.get_secret_value(),
    )


# --------------------------------------------------------------------------
# Query construction
# --------------------------------------------------------------------------
def build_feature_search(
    config: PullEventsConfig, since: datetime.datetime
) -> client.FeatureSearchRequest:
    """The one search a run makes.

    The scope half narrows which Prediction Results are read; the feature half
    carries the watermark, the area of interest and any confidence threshold.

    The area of interest goes on the *features*, not on
    `prediction_intersects_geometry`. The Result-level geometry filter matches
    through registered Areas only — a Prediction created from an uploaded
    GeoJSON keeps its footprint in a file and never matches — so using it to
    enforce an AOI would silently drop exactly the detections nobody would
    think to look for. `target_area_id` is the field for pruning by area, and
    it is a separate, explicit setting.
    """
    scope = client.PredictionResultFilters()
    if config.model_id:
        scope.prediction_model_id = client.KeywordFilter(eq=config.model_id)
    if config.project_id:
        scope.prediction_project_id = client.KeywordFilter(eq=config.project_id)
    if config.organization_id:
        scope.organization_id = client.KeywordFilter(eq=config.organization_id)
    if config.target_area_id:
        scope.prediction_target_area_id = client.KeywordFilter(eq=config.target_area_id)

    features = client.FeatureFilters(
        limit=config.feature_page_size,
        oe_created_at=client.DatetimeFilter(gte=since),
    )
    if config.area_of_interest:
        try:
            features.intersects_geometry = client.Geometry.parse_obj(config.area_of_interest)
        except pydantic.ValidationError as e:
            raise IntegrationConfigurationError(
                "The configured area of interest is not a valid GeoJSON geometry; "
                "it needs a `type` and matching `coordinates`."
            ) from e
    if config.min_confidence is not None:
        features.property_filters = [
            client.PropertyFilter(
                property_name=config.confidence_property,
                numeric_filter=client.NumericFilter(gte=config.min_confidence),
            )
        ]

    return client.FeatureSearchRequest(prediction_results=scope, features=features)


# --------------------------------------------------------------------------
# Geometry
# --------------------------------------------------------------------------
def _flatten_positions(coordinates: Any) -> Iterable[Tuple[float, float]]:
    """Yield every (lon, lat) position in an arbitrarily nested coordinate array."""
    if not isinstance(coordinates, (list, tuple)) or not coordinates:
        return
    first = coordinates[0]
    if isinstance(first, (int, float)) and len(coordinates) >= 2:
        yield float(coordinates[0]), float(coordinates[1])
        return
    for item in coordinates:
        for position in _flatten_positions(item):
            yield position


def _bbox_centre(bbox: List[float]) -> Optional[Dict[str, float]]:
    """The centre of a GeoJSON bounding box, 2-D or 3-D.

    RFC 7946 §5 orders a bbox as *every* minimum followed by *every* maximum,
    so a 3-D box is [west, south, minAlt, east, north, maxAlt]. Read as a flat
    [minLon, minLat, maxLon, maxLat] its minimum altitude lands in the
    longitude — an altitude in metres plotted as a degree, which puts the event
    anywhere from mid-ocean to off the map.
    """
    if len(bbox) < 4 or len(bbox) % 2:
        return None
    axes = len(bbox) // 2
    min_lon, min_lat = bbox[0], bbox[1]
    max_lon, max_lat = bbox[axes], bbox[axes + 1]
    if min_lon > max_lon:
        # §5.2: a box straddling the antimeridian has west > east. Averaged as
        # it stands, the centre lands on the opposite side of the globe.
        max_lon += 360.0
    elif max_lon - min_lon > 180.0:
        # The same shape arriving as loose positions rather than a declared
        # bbox: spanning more than half the globe is the signal that it wraps.
        min_lon, max_lon = max_lon, min_lon + 360.0
    lon = (min_lon + max_lon) / 2
    if lon > 180.0:
        lon -= 360.0
    return {"lat": (min_lat + max_lat) / 2, "lon": lon}


def centroid_of(feature: client.Feature) -> Optional[Dict[str, float]]:
    """A single lat/lon for a feature of any geometry type.

    Gundi's event location is a point, but a detection is often a polygon, so
    one has to be derived. The feature's own `bbox` is preferred — the provider
    computed it over the real geometry — and averaging the positions is the
    fallback. Neither is a true centroid for a concave shape; the full geometry
    travels alongside for anything that needs the real footprint.
    """
    bbox = feature.bbox or (feature.geometry.bbox if feature.geometry else None)
    centre = _bbox_centre(bbox) if bbox else None
    if centre:
        return centre
    if not feature.geometry:
        return None
    # Reduce the positions to a bounding box and go through the same function,
    # so there is exactly one place that knows longitudes wrap. Averaging the
    # vertices directly put a shape spanning the antimeridian at longitude 0.
    min_lon = min_lat = max_lon = max_lat = None
    for lon, lat in _flatten_positions(feature.geometry.coordinates):
        if min_lon is None:
            min_lon = max_lon = lon
            min_lat = max_lat = lat
            continue
        min_lon, max_lon = min(min_lon, lon), max(max_lon, lon)
        min_lat, max_lat = min(min_lat, lat), max(max_lat, lat)
    if min_lon is None:
        return None
    return _bbox_centre([min_lon, min_lat, max_lon, max_lat])


# --------------------------------------------------------------------------
# Transformation
# --------------------------------------------------------------------------
def transform_feature(feature: client.Feature, config: PullEventsConfig) -> Optional[dict]:
    """One OlmoEarth feature as one Gundi event, or None if it cannot be placed.

    `recorded_at` is the observation's own `oe_start_time` — when the model saw
    the thing — falling back to `oe_created_at`, when the record was written.
    They differ by however long the imagery took to process, which for a
    detection feed is the difference between a useful timestamp and a useless
    one.
    """
    if feature.id is None:
        # Everything downstream is keyed on external_source_id, and without an
        # id there is nothing stable to build one from: every id-less feature
        # in a Result would share one identity, colliding in Gundi and in this
        # run's own boundary set.
        logger.warning(
            "Skipping a feature from prediction result %s: it carries no id, so "
            "it has no stable external_source_id.",
            feature.properties.oe_prediction_result_id,
        )
        return None

    location = centroid_of(feature)
    if not location:
        logger.warning(
            "Skipping feature %s: it carries no geometry or bounding box, so it "
            "cannot be placed on a map.",
            feature.id,
        )
        return None

    properties = feature.properties
    recorded_at = properties.oe_start_time or properties.oe_created_at
    if not recorded_at:
        logger.warning(
            "Skipping feature %s: it has neither oe_start_time nor oe_created_at, "
            "and an event needs a timestamp.",
            feature.id,
        )
        return None

    event_details = properties.model_properties()
    event_details["feature_id"] = feature.id
    if properties.oe_prediction_result_id:
        event_details["prediction_result_id"] = properties.oe_prediction_result_id
    if properties.oe_prediction_result_file_id:
        event_details["prediction_result_file_id"] = properties.oe_prediction_result_file_id
    if config.model_id:
        event_details["model_id"] = config.model_id
    if properties.oe_end_time:
        event_details["observation_end_time"] = properties.oe_end_time.isoformat()
    if properties.oe_created_at:
        event_details["detected_at"] = properties.oe_created_at.isoformat()

    event = {
        "title": _event_title(feature, config),
        "event_type": config.event_type,
        "recorded_at": recorded_at.isoformat(),
        "location": location,
        "event_details": event_details,
        # Stable across runs, so a redelivered feature is recognisable as the
        # same detection rather than a second one.
        "external_source_id": external_id_for(feature),
    }
    if config.include_geometry and feature.geometry:
        # Not `.dict()`: `coordinates` is typed Any, so pydantic deep-copies the
        # whole nested array for every feature. No field under Geometry is a
        # model, so taking the set values as they are gives the same dict.
        event["geometry"] = {
            name: value
            for name, value in feature.geometry.__dict__.items()
            if value is not None
        }
    return event


def external_id_for(feature: client.Feature) -> str:
    """A feature id is only unique within its Prediction Result, so qualify it.

    A cross-Result search returns features from many Results at once, which is
    exactly when an unqualified id would collide. The Result id travels on the
    feature's own properties, so no extra lookup is needed for it.
    """
    result_id = feature.properties.oe_prediction_result_id
    return f"{result_id}:{feature.id}" if result_id else str(feature.id)


def _event_title(feature: client.Feature, config: PullEventsConfig) -> str:
    label = config.event_title_prefix or "OlmoEarth"
    return f"{label} detection {feature.id}" if feature.id is not None else f"{label} detection"


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------
class Watermark:
    """How far the ingest has read, and what it has already sent at that instant.

    `created_at` is the newest `oe_created_at` ingested. The boundary set holds
    the identities already sent carrying exactly that timestamp — what the next
    run's `gte` re-read has to subtract.

    It takes `(created_at, external_id)` pairs rather than features: a cursor is
    defined over those two values, and a feature that cannot supply both is not
    this class's problem to detect. One insertion-ordered dict serves as the set
    — membership is O(1), and the order is what `to_state()`'s truncation keeps.

    An identity is the *qualified* id, the same `external_source_id` the event
    carries. A bare feature id is unique only within its Prediction Result, and
    one search spans many Results at once: keyed on the bare id, feature 1 of
    result-2 would look like feature 1 of result-1 and be dropped.
    """

    def __init__(
        self,
        created_at: Optional[datetime.datetime] = None,
        boundary_ids: Optional[Iterable[str]] = None,
        memory: int = BOUNDARY_ID_MEMORY,
    ):
        self.created_at = created_at
        self._boundary: Dict[str, None] = dict.fromkeys(boundary_ids or [])
        self.memory = memory
        self.dirty = False
        self.overflowed = False

    @property
    def boundary_ids(self) -> List[str]:
        return list(self._boundary)

    def already_ingested(self, created_at: Optional[datetime.datetime], external_id: str) -> bool:
        """True for an identity this watermark has already accounted for."""
        if self.created_at is None or created_at is None:
            return False
        return _as_utc(created_at) == self.created_at and external_id in self._boundary

    def advance(self, created_at: Optional[datetime.datetime], external_id: str) -> None:
        """Record that `external_id` has been ingested."""
        if created_at is None:
            # Nothing to advance to. The detection is still sent; it just cannot
            # move a cursor defined in terms of a field it lacks.
            return
        created_at = _as_utc(created_at)
        self.dirty = True
        if self.created_at is None or created_at > self.created_at:
            self.created_at = created_at
            # Identities at an instant the cursor has passed are dead weight:
            # `gte` on a later timestamp already excludes them.
            self._boundary = {external_id: None}
        elif created_at == self.created_at:
            self._boundary[external_id] = None

    def to_state(self) -> dict:
        """The persisted form, with the boundary set bounded.

        Truncation is deliberately in the duplicate direction: a forgotten
        identity at the boundary second is re-read and re-sent next run, where
        a `gt` cursor would have skipped it silently. Gundi sees the same
        `external_source_id` twice, which is recoverable; a missing detection
        is not.
        """
        boundary = self.boundary_ids
        if len(boundary) > self.memory and not self.overflowed:
            self.overflowed = True
            logger.warning(
                "More than %s detections share the watermark second %s; only the "
                "most recent %s are remembered, so the rest are re-sent next run "
                "as duplicate events with the same external_source_id.",
                self.memory,
                self.created_at.isoformat() if self.created_at else None,
                self.memory,
            )
        return {
            "last_feature_created_at": self.created_at.isoformat() if self.created_at else None,
            "boundary_feature_ids": boundary[-self.memory :],
            "updated_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        }


async def _load_watermark(integration_id: str) -> Watermark:
    state = await state_manager.get_state(integration_id, PULL_EVENTS_ACTION_ID) or {}
    last_seen = state.get("last_feature_created_at")
    parsed = None
    if last_seen:
        try:
            parsed = _parse_datetime(last_seen)
        except ValueError:
            logger.warning(
                "Ignoring unparseable watermark %r for integration %s; falling back "
                "to the configured lookback.",
                last_seen, integration_id,
            )
    return Watermark(parsed, state.get("boundary_feature_ids") or [])


async def _save_watermark(integration_id: str, watermark: Watermark) -> None:
    """Persist the cursor, unless nothing has moved it since the last write.

    An idle run reads only re-reads, advances nothing, and would otherwise
    write back the state it just loaded — which on a crowded boundary second is
    a few hundred kilobytes of identical ids, six times a day, per integration.
    """
    if not watermark.dirty:
        return
    await state_manager.set_state(integration_id, PULL_EVENTS_ACTION_ID, watermark.to_state())
    watermark.dirty = False


def _parse_datetime(value: str) -> datetime.datetime:
    return _as_utc(pydantic.datetime_parse.parse_datetime(value))


def _as_utc(value: datetime.datetime) -> datetime.datetime:
    """Compare watermarks in one timezone; a naive provider timestamp is UTC."""
    return value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)


# --------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------
@action_title("Authenticate")
async def action_auth(integration, action_config: AuthenticateConfig) -> dict:
    """Check that the token and site URL actually reach the OlmoEarth API.

    This asks the *predictions* search rather than the features search the
    pull action uses, and that is on purpose: the features endpoint accepts
    anonymous callers, so a dead token would come back 200 with the public
    Results instead of a 401. The predictions search requires a real user, so
    it gives a straight answer about the token.
    """
    logger.info(f"Executing auth action for integration {integration.id}...")
    async with client_for(integration, action_config) as api:
        response = await api.search_predictions(
            client.PredictionSearchRequest(limit=1, sort_by="creation_time", sort_direction="desc")
        )
    return {
        "valid_credentials": True,
        "predictions_visible": response.meta.total,
    }


@action_title("List Predictions")
async def action_list_predictions(integration, action_config: ListPredictionsConfig) -> dict:
    """Reference lookup: the predictions the portal offers as a dropdown.

    Stateless by contract — the query arrives as config overrides when a user
    opens the dropdown and nothing is stored.
    """
    request = client.PredictionSearchRequest(
        limit=action_config.limit, sort_by="creation_time", sort_direction="desc"
    )
    if action_config.model_id:
        request.model_id = client.KeywordFilter(eq=action_config.model_id)
    if action_config.project_id:
        request.project_id = client.KeywordFilter(eq=action_config.project_id)
    if action_config.target_area_id:
        request.target_area_id = client.KeywordFilter(eq=action_config.target_area_id)
    if action_config.status:
        request.status = client.KeywordFilter(eq=action_config.status)

    async with client_for(integration) as api:
        response = await api.search_predictions(request)

    options = [
        ReferenceOption(
            value=prediction.id,
            label=prediction.name or prediction.id,
            description=_prediction_description(prediction),
            group=prediction.model_id,
        )
        for prediction in response.records
    ]
    total = response.meta.total
    return ReferenceDataResponse(
        options=options,
        truncated=bool(total is not None and total > len(options)),
    ).dict()


def _prediction_description(prediction: client.Prediction) -> Optional[str]:
    parts = []
    if prediction.status:
        parts.append(prediction.status)
    if prediction.creation_time:
        parts.append(f"created {prediction.creation_time.isoformat()}")
    return " · ".join(parts) or None


@crontab_schedule("0 */4 * * *")  # Every four hours
@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig) -> dict:
    """Ingest every detection this integration has not already seen."""
    integration_id = str(integration.id)
    logger.info(f"Executing pull_events for integration {integration_id}...")

    watermark = await _load_watermark(integration_id)
    since = watermark.created_at or (
        datetime.datetime.now(tz=datetime.timezone.utc)
        - datetime.timedelta(days=action_config.lookback_days)
    )
    request = build_feature_search(action_config, since)

    totals = {"features_read": 0, "features_skipped": 0, "events_sent": 0, "truncated": False}
    batch: List[dict] = []

    async with client_for(integration) as api:
        try:
            async for feature in api.iter_features(request):
                created_at = feature.properties.oe_created_at
                if totals["features_read"] >= action_config.max_features_per_run and not (
                    _same_instant(created_at, watermark.created_at)
                ):
                    # The cap is reached, and this detection belongs to a later
                    # second than the one being read — a safe place to stop.
                    logger.info(
                        "Stopped at the %s-detection cap for this run; the watermark "
                        "carries the rest into the next one.",
                        action_config.max_features_per_run,
                    )
                    break

                event = transform_feature(feature, action_config)
                if event is None:
                    # Unusable: no id, no geometry, or no timestamp. It still
                    # counts as read, so the cursor can move past it.
                    totals["features_read"] += 1
                    watermark.advance(created_at, _unusable_id(feature))
                    continue

                external_id = event["external_source_id"]
                if watermark.already_ingested(created_at, external_id):
                    # Re-read because the search is bounded with `gte`. Expected,
                    # not an anomaly: every run re-reads the watermark second, and
                    # a re-read is not work, so it does not count against the cap.
                    totals["features_skipped"] += 1
                    continue

                totals["features_read"] += 1
                watermark.advance(created_at, external_id)
                batch.append(event)
                if len(batch) >= action_config.events_per_request:
                    totals["events_sent"] += await _send_events(batch, integration_id)
                    batch = []
                    # Persisted per batch, not only at the end: a run that
                    # fails on the fifth batch of six must not re-send the
                    # first four.
                    await _save_watermark(integration_id, watermark)
        except client.FeatureWalkTruncated as truncation:
            # The walk hit its page ceiling with matches unread. Everything
            # yielded so far is good and is kept; the run just is not complete,
            # and saying so is the difference between a visible problem and a
            # stuck feed reporting a quiet day.
            totals["truncated"] = True
            logger.warning("%s Narrow the query to see the rest.", truncation)

        if batch:
            totals["events_sent"] += await _send_events(batch, integration_id)

        # Unconditional, and not only when something was sent: a run whose
        # features all failed to transform still read them, and dropping that
        # progress means re-reading the same unusable features every run —
        # forever, and never past them once there are a capful.
        await _save_watermark(integration_id, watermark)

        if not totals["events_sent"] and not totals["truncated"]:
            # An empty run is ambiguous on this endpoint: it accepts anonymous
            # callers, so an expired token returns 200 with only the public
            # Results — which for a private feed is zero detections and no
            # error. Ask an endpoint that does require auth before calling it
            # a quiet day.
            await api.verify_token()

    await _log_run(integration_id, action_config, since, totals)

    totals["since"] = since.isoformat()
    totals["watermark"] = watermark.created_at.isoformat() if watermark.created_at else None
    return totals


def _same_instant(
    created_at: Optional[datetime.datetime], cursor: Optional[datetime.datetime]
) -> bool:
    """Whether a detection belongs to the second the cursor currently names.

    This is what makes the run cap safe. Stopping mid-second leaves the cursor
    inside a group that `gte` will re-read in full, so the next run re-reads
    what this one already sent and — once a second holds more detections than
    the boundary set remembers — the two runs trade halves forever without ever
    reaching what lies past them. Finishing the second the cap fell in costs an
    overrun bounded by how many detections share one timestamp, and in exchange
    the cursor always lands *between* groups, where `gte` re-reads nothing that
    was not fully drained.
    """
    if created_at is None or cursor is None:
        return False
    return _as_utc(created_at) == cursor


def _unusable_id(feature: client.Feature) -> str:
    """A boundary identity for a detection that will never become an event.

    It only has to be distinct from every real `external_source_id` and from
    the other unusable detections at its instant, so that one cannot suppress
    another. Nothing downstream ever sees it.
    """
    return f"unusable:{feature.properties.oe_prediction_result_id}:{id(feature)}"


async def _log_run(
    integration_id: str, config: PullEventsConfig, since: datetime.datetime, totals: dict
) -> None:
    if totals["events_sent"]:
        title = f"Ingested {totals['events_sent']} detection(s)."
    else:
        title = "No new detections to ingest."
    await log_action_activity(
        integration_id=integration_id,
        action_id=PULL_EVENTS_ACTION_ID,
        level="INFO",
        title=title,
        data={
            "since": since.isoformat(),
            "features_read": totals["features_read"],
            "features_skipped": totals["features_skipped"],
            "events_sent": totals["events_sent"],
        },
        config_data=config.dict(),
    )


async def _send_events(events: List[dict], integration_id: str) -> int:
    """Send one batch and report how many events it carried.

    `send_events_to_gundi` already retries transient Gundi failures; a failure
    that survives that propagates, which fails the action and leaves the
    watermark where the last successful batch put it, so the next run resumes
    there.
    """
    logger.info(f"Sending {len(events)} event(s) to Gundi for integration {integration_id}...")
    await gundi_tools.send_events_to_gundi(events=events, integration_id=integration_id)
    return len(events)
