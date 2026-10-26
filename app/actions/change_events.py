"""Validate polygon changes, match repeat detections, and build Gundi event payloads."""

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional
from uuid import NAMESPACE_URL, uuid5

from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from app.services.errors import IntegrationBadResponseError

from .client import Feature
from .configurations import polygon_shape
from .monitor_state import Job, KnownEvent

class InvalidFeatureError(IntegrationBadResponseError):
    """A detection in a result is unusable; retrying the same page cannot fix it."""


def parse_change_date(value) -> date:
    if not isinstance(value, str):
        raise InvalidFeatureError(
            "LCC change date must be an ISO date string; encoded months and timestep indices are unsupported."
        )
    try:
        if len(value) == 10:
            return date.fromisoformat(value)
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("Missing timezone")
        return stamp.astimezone(timezone.utc).date()
    except ValueError as exc:
        raise InvalidFeatureError(
            "LCC change date is missing or is not an ISO date/time."
        ) from exc


def event_id(integration_id: str, job: Job, feature: Feature) -> str:
    """The stable Gundi external ID for one detection in one prediction result."""
    return str(uuid5(NAMESPACE_URL, f"{integration_id}:{job.result_id}:{feature.id}"))


def classify(feature: Feature, job: Job) -> Optional[tuple[str, date]]:
    """(change type, change date) for a detection the job monitors, else None.

    Raises InvalidFeatureError if a monitored detection has no usable date.
    """
    props = feature.properties.model_properties()
    change_type = props.get("post_change")
    if change_type not in job.config.selected_types:
        return None
    changed = parse_change_date(props.get(job.config.change_date_property))
    if job.config.changes_since and changed < job.config.changes_since:
        return None
    return change_type, changed


def candidate_windows(features: list[Feature], job: Job) -> dict[str, tuple[date, date]]:
    """Per change type, the change-date range any repeat of these features can fall in.

    Features that prepare_event would skip or reject are ignored here; it reports them.
    """
    dates: dict[str, list[date]] = {}
    for feature in features:
        try:
            found = classify(feature, job)
        except InvalidFeatureError:
            continue
        if found:
            change_type, changed = found
            dates.setdefault(change_type, []).append(changed)
    tolerance = timedelta(days=job.config.date_tolerance_days)
    return {
        change_type: (min(found) - tolerance, max(found) + tolerance)
        for change_type, found in dates.items()
    }


class EventStage:
    """One page's view of event history: committed candidates plus staged changes.

    Nothing here is persisted until the page's outbox is accepted and committed.
    """

    def __init__(self, candidates: list[KnownEvent], known_ids: set[str]):
        # Copies, so a page that is never committed leaves no trace.
        self.candidates = [event.copy() for event in candidates]
        self.known_ids = set(known_ids)
        self.new: list[KnownEvent] = []
        self.updated: dict[str, KnownEvent] = {}


def prepare_event(
    feature: Feature,
    job: Job,
    stage: EventStage,
    shapes: dict[str, BaseGeometry],
    *,
    integration_id: str,
    now: datetime,
) -> Optional[dict]:
    """Build a new event or record a repeat in the page's staged history.

    Mutates only ``stage`` and ``shapes``, a cache of known-event geometries keyed
    by external ID. This function performs no persistence or network operations.
    """
    if feature.id is None or feature.geometry is None:
        raise InvalidFeatureError(
            f"Detection {feature.id} has no stable feature ID or polygon geometry."
        )
    found = classify(feature, job)
    if found is None:
        return None
    change_type, changed = found
    geometry = feature.geometry.dict(exclude_none=True)
    try:
        geom = polygon_shape(geometry)
    except ValueError as exc:
        raise InvalidFeatureError(
            f"Detection {feature.id} geometry is not a valid regional polygon."
        ) from exc
    external_id = event_id(integration_id, job, feature)
    if external_id in stage.known_ids:
        return None
    matches = []
    for previous in stage.candidates:
        # Detections from the same run are distinct polygons, never repeats.
        if (
            previous.change_type != change_type
            or previous.first_prediction_id == job.prediction_id
        ):
            continue
        if abs((previous.change_date - changed).days) > job.config.date_tolerance_days:
            continue
        old_geom = shapes.get(previous.external_id)
        if old_geom is None:
            old_geom = shapes[previous.external_id] = shape(previous.geometry)
        if not geom.intersects(old_geom):
            continue
        # Both are valid polygons, so the union's area follows from the overlap.
        overlap = geom.intersection(old_geom).area
        union = geom.area + old_geom.area - overlap
        iou = overlap / union if union else 0
        if iou >= job.config.overlap_threshold:
            matches.append(previous)
    if len(matches) == 1:
        previous = matches[0]
        previous.last_seen_at = now
        # A polygon split into several features this run is still one observation.
        if previous.last_prediction_id != job.prediction_id:
            previous.last_prediction_id = job.prediction_id
            previous.observations += 1
        stage.updated[previous.external_id] = previous
        return None
    stage.known_ids.add(external_id)
    stage.new.append(
        KnownEvent(
            external_id=external_id,
            geometry=geometry,
            change_type=change_type,
            change_date=changed,
            first_seen_at=now,
            last_seen_at=now,
            first_prediction_id=job.prediction_id,
            last_prediction_id=job.prediction_id,
        )
    )
    shapes[external_id] = geom
    point = geom.representative_point()
    props = feature.properties.model_properties()
    props.update(
        {
            "prediction_id": job.prediction_id,
            "prediction_result_id": job.result_id,
            "oerun_prediction_result_id": feature.properties.oe_prediction_result_id,
            "external_event_id": external_id,
            "feature_id": feature.id,
            "model_id": str(job.config.model_id),
            "first_discovered_at": now.isoformat(),
            "estimated_change_date": changed.isoformat(),
            "change_date_precision": job.config.change_date_precision,
            "change_date_aggregation": job.config.change_date_aggregation,
            "prediction_reference_time": job.reference_time,
            "ambiguous_match_count": len(matches),
        }
    )
    return {
        "title": f"OlmoEarth: {change_type.replace('_', ' ')}",
        "event_type": job.config.event_type,
        "source": f"olmoearth-monitor:{integration_id}",
        "recorded_at": datetime.combine(changed, time(), timezone.utc).isoformat(),
        "location": {"lat": point.y, "lon": point.x},
        "geometry": geometry,
        "event_details": props,
    }
