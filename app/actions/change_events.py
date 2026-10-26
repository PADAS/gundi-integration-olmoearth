"""Validate polygon changes, match repeat detections, and build Gundi event payloads."""

from datetime import date, datetime, time, timezone
from typing import Optional
from uuid import NAMESPACE_URL, uuid5

from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from app.services.errors import (
    IntegrationBadResponseError,
    IntegrationConfigurationError,
)

from .client import Feature
from .configurations import polygon_shape
from .monitor_state import Job, KnownEvent

MAX_EVENTS = 10000


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


def prepare_event(
    feature: Feature,
    job: Job,
    known: list[KnownEvent],
    shapes: dict[str, BaseGeometry],
    *,
    integration_id: str,
    now: datetime,
) -> Optional[dict]:
    """Build a new event or record a repeat in the caller's staged history.

    Mutates only ``known``, the page-local copy committed with the delivery outbox,
    and ``shapes``, a cache of known-event geometries keyed by external ID.
    This function performs no persistence or network operations.
    """
    if feature.id is None or feature.geometry is None:
        raise InvalidFeatureError(
            f"Detection {feature.id} has no stable feature ID or polygon geometry."
        )
    props = feature.properties.model_properties()
    change_type = props.get("post_change")
    if change_type not in job.config.selected_types:
        return None
    changed = parse_change_date(props.get(job.config.change_date_property))
    if job.config.changes_since and changed < job.config.changes_since:
        return None
    geometry = feature.geometry.dict(exclude_none=True)
    try:
        geom = polygon_shape(geometry)
    except ValueError as exc:
        raise InvalidFeatureError(
            f"Detection {feature.id} geometry is not a valid regional polygon."
        ) from exc
    external_id = str(
        uuid5(NAMESPACE_URL, f"{integration_id}:{job.result_id}:{feature.id}")
    )
    if any(event.external_id == external_id for event in known):
        return None
    matches = []
    for previous in known:
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
        union = geom.union(old_geom).area
        iou = geom.intersection(old_geom).area / union if union else 0
        if iou >= job.config.overlap_threshold:
            matches.append(previous)
    if len(matches) == 1:
        previous = matches[0]
        previous.last_seen_at = now
        # A polygon split into several features this run is still one observation.
        if previous.last_prediction_id != job.prediction_id:
            previous.last_prediction_id = job.prediction_id
            previous.observations += 1
        return None
    if len(known) >= MAX_EVENTS:
        raise IntegrationConfigurationError(
            "Prototype event history is full; migrate to a durable event store before collecting more."
        )
    known.append(
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
            "prediction_reference_time": job.request["start_time"],
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
