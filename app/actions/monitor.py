"""Persisted prediction jobs and a resumable event outbox for one regional LCC monitor."""

import calendar
import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Awaitable, Callable, Optional
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, Field
from shapely.geometry import shape

from app.services.errors import (
    IntegrationBadResponseError,
    IntegrationConfigurationError,
)

from .client import (
    Feature,
    FeatureSearchRequest,
    KeywordFilter,
    OlmoEarthClient,
    PredictionSearchRequest,
    PropertyFilter,
    StringFilter,
)
from .configurations import Cadence, PullEventsConfig, polygon_geometry

STATE_ACTION = "lcc_monitor_v1"
MAX_JOBS = 1000
MAX_EVENTS = 10000
RESULT_WINDOW = 10000


class Job(BaseModel):
    name: str
    request: dict
    config: PullEventsConfig
    prediction_id: Optional[str] = None
    result_id: Optional[str] = None
    status: str = "submitting"
    offset: int = 0
    error: Optional[str] = None


class KnownEvent(BaseModel):
    external_id: str
    geometry: dict
    change_type: str
    change_date: date
    first_seen_at: datetime
    last_seen_at: datetime
    last_prediction_id: str
    observations: int = 1
    gundi_object_id: Optional[str] = None


class Outbox(BaseModel):
    batch_id: str
    delivery_status: str = "pending"
    receipts: list[str] = Field(default_factory=list)
    job_name: str
    events: list[dict]
    known_events: list[KnownEvent]
    next_offset: int
    complete: bool


class MonitorState(BaseModel):
    identity: str
    next_slot: int = 0
    jobs: list[Job] = Field(default_factory=list)
    known_events: list[KnownEvent] = Field(default_factory=list)
    outbox: Optional[Outbox] = None


def monitor_identity(config: PullEventsConfig) -> str:
    fields = {
        "project_id",
        "organization_id",
        "model_id",
        "area_of_interest",
        "schedule_start",
        "cadence",
        "imagery_lag_days",
        "change_date_property",
        "change_date_aggregation",
        "change_date_precision",
        "event_type",
    }
    return hashlib.sha256(
        config.json(include=fields, sort_keys=True).encode()
    ).hexdigest()


def schedule_slot(config: PullEventsConfig, index: int) -> date:
    anchor = config.schedule_start
    if config.cadence == Cadence.WEEKLY:
        return anchor + timedelta(weeks=index)
    months = {Cadence.MONTHLY: 1, Cadence.QUARTERLY: 3, Cadence.ANNUAL: 12}[
        config.cadence
    ] * index
    year, month = divmod(anchor.year * 12 + anchor.month - 1 + months, 12)
    month += 1
    return date(year, month, min(anchor.day, calendar.monthrange(year, month)[1]))


def parse_change_date(value) -> date:
    if not isinstance(value, str):
        raise IntegrationBadResponseError(
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
        raise IntegrationBadResponseError(
            "LCC change date is missing or is not an ISO date/time."
        ) from exc


def build_feature_query(config: PullEventsConfig, offset: int) -> FeatureSearchRequest:
    return FeatureSearchRequest(
        limit=config.feature_page_size,
        offset=offset,
        intersects_geometry=config.area_of_interest,
        property_filters=[
            PropertyFilter(
                property_name="post_change",
                keyword_filter=KeywordFilter(inc=config.selected_types),
            )
        ],
    )


class Monitor:
    def __init__(
        self,
        integration_id: str,
        config: PullEventsConfig,
        api: OlmoEarthClient,
        state_manager,
        send: Callable[[list[dict]], Awaitable[list[str]]],
        now: datetime,
    ):
        self.integration_id = integration_id
        self.config = config
        self.api = api
        self.store = state_manager
        self.send = send
        self.now = now.astimezone(timezone.utc)
        self.state: MonitorState

    async def load(self):
        raw = await self.store.get_state(self.integration_id, STATE_ACTION)
        identity = monitor_identity(self.config)
        self.state = (
            MonitorState.parse_obj(raw) if raw else MonitorState(identity=identity)
        )
        if self.state.identity != identity:
            raise IntegrationConfigurationError(
                "Model, area, project, date semantics or schedule changed. Create a new monitor integration to preserve existing jobs and event identity."
            )

    async def save(self):
        await self.store.set_state(
            self.integration_id, STATE_ACTION, json.loads(self.state.json())
        )

    async def submit(self) -> dict:
        await self.load()
        if str(self.config.organization_id or ""):
            organization = await self.api.project_organization(
                str(self.config.project_id)
            )
            if organization != str(self.config.organization_id):
                raise IntegrationConfigurationError(
                    "The selected project does not belong to the configured organization."
                )
        uncertain = next((j for j in self.state.jobs if j.status == "submitting"), None)
        if uncertain:
            # Search names include this integration and schedule slot; never retry the POST.
            response = await self.api.search_predictions(
                PredictionSearchRequest(
                    project_id=KeywordFilter(eq=str(self.config.project_id)),
                    model_id=KeywordFilter(eq=str(self.config.model_id)),
                    name=StringFilter(eq=uncertain.name),
                    limit=2,
                )
            )
            if len(response.records) != 1 or (response.meta.total or 0) > 1:
                raise IntegrationConfigurationError(
                    "A prediction submission has an unknown outcome. Reconcile its saved request name before resubmitting; automatic retries could create duplicate predictions."
                )
            uncertain.prediction_id = response.records[0].id
            uncertain.status = "pending"
            await self.save()
            return {"recovered_prediction_id": uncertain.prediction_id}
        if any(
            j.status in {"pending", "collecting", "needs_export"}
            for j in self.state.jobs
        ):
            return {"status": "waiting_for_existing_prediction"}
        slot = schedule_slot(self.config, self.state.next_slot)
        if slot > self.now.date():
            return {"status": "not_due", "next_prediction_date": slot.isoformat()}
        if len(self.state.jobs) >= MAX_JOBS:
            raise IntegrationConfigurationError(
                "Prototype job history is full; archive the monitor before submitting more runs."
            )
        reference = datetime.combine(
            slot - timedelta(days=self.config.imagery_lag_days), time(), timezone.utc
        ).isoformat()
        name = f"gundi:{self.integration_id}:{slot.isoformat()}"
        body = {
            "name": name,
            "project_id": str(self.config.project_id),
            "model_id": str(self.config.model_id),
            "start_time": reference,
            "end_time": reference,
            "geojson": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": self.config.area_of_interest,
                        "properties": {"start_time": reference, "end_time": reference},
                    }
                ],
            },
        }
        job = Job(name=name, request=body, config=self.config.copy(deep=True))
        self.state.jobs.append(job)
        self.state.next_slot += 1
        await self.save()
        prediction = await self.api.create_prediction(body)
        job.prediction_id = prediction.id
        job.status = "pending"
        await self.save()
        return {"submitted_prediction_id": prediction.id, "reference_date": reference}

    async def flush_outbox(self) -> int:
        box = self.state.outbox
        if box is None:
            return 0
        if box.delivery_status == "sending":
            raise IntegrationConfigurationError(
                f"Delivery outcome is unknown for batch {box.batch_id}. Use Resolve LCC Delivery after checking Gundi; it will not be resent automatically."
            )
        if box.events and box.delivery_status == "pending":
            box.delivery_status = "sending"
            await self.save()
            box.receipts = await self.send(box.events)
            if len(box.receipts) != len(box.events):
                raise IntegrationBadResponseError(
                    "Gundi did not acknowledge every event in the outbox."
                )
            box.delivery_status = "accepted"
            await self.save()
        for event, object_id in zip(box.events, box.receipts):
            identity = event["event_details"]["external_event_id"]
            next(
                record for record in box.known_events if record.external_id == identity
            ).gundi_object_id = object_id
        self.state.known_events = box.known_events
        job = next(j for j in self.state.jobs if j.name == box.job_name)
        job.offset = box.next_offset
        job.status = "complete" if box.complete else "collecting"
        self.state.outbox = None
        await self.save()
        return len(box.events)

    async def resolve_delivery(self, resolution) -> dict:
        await self.load()
        box = self.state.outbox
        if (
            box is None
            or box.batch_id != str(resolution.batch_id)
            or box.delivery_status != "sending"
        ):
            raise IntegrationConfigurationError(
                "That batch is not awaiting delivery reconciliation."
            )
        if resolution.outcome == "confirmed_delivered":
            ids = resolution.gundi_object_ids
            if (
                len(ids) != len(box.events)
                or len(set(ids)) != len(ids)
                or any(not value.strip() for value in ids)
            ):
                raise IntegrationConfigurationError(
                    "Provide one distinct Gundi object ID for every outbox event, in order."
                )
            box.receipts = ids
            box.delivery_status = "accepted"
        else:
            if resolution.gundi_object_ids:
                raise IntegrationConfigurationError(
                    "A confirmed undelivered batch must not have Gundi object IDs."
                )
            box.delivery_status = "pending"
        await self.save()
        return {"batch_id": box.batch_id, "status": box.delivery_status}

    async def collect(self) -> dict:
        await self.load()
        await self.api.verify_token()
        sent = await self.flush_outbox()
        pages = 0
        for job in self.state.jobs:
            if job.status not in {"pending", "collecting"}:
                continue
            prediction = await self.api.get_prediction(job.prediction_id)
            if prediction.status in {"failed", "cancelled"}:
                job.status = "failed"
                job.error = prediction.error_message or prediction.status
                await self.save()
                continue
            if prediction.status != "completed":
                continue
            if prediction.result is None or "mvt" not in prediction.result.file_formats:
                raise IntegrationConfigurationError(
                    "Completed prediction has no vector result. Use an LCC model configured with vectorization."
                )
            if job.result_id is not None and job.result_id != prediction.result.id:
                raise IntegrationBadResponseError(
                    "Prediction result changed while paging; reconciliation is required."
                )
            job.result_id = prediction.result.id
            job.status = "collecting"
            await self.save()
            while pages < self.config.pages_per_poll and job.status == "collecting":
                if job.offset >= RESULT_WINDOW:
                    job.status = "needs_export"
                    job.error = "Result reaches the 10,000-feature search window; completeness requires an export or a smaller AOI."
                    await self.save()
                    break
                query = build_feature_query(job.config, job.offset)
                query.limit = min(query.limit, RESULT_WINDOW - job.offset)
                response = await self.api.search_features(job.result_id, query)
                if (
                    response.meta.total is not None
                    and response.meta.total > RESULT_WINDOW
                ):
                    job.status = "needs_export"
                    job.error = "Result exceeds the feature search window; use a smaller AOI or export."
                    await self.save()
                    break
                # A short page proves exhaustion. ES totals can be lower bounds at 10,000.
                complete = len(response.records) < query.limit
                known = [event.copy(deep=True) for event in self.state.known_events]
                events = []
                for feature in response.records:
                    event = self.prepare_event(feature, job, known)
                    if event:
                        events.append(event)
                self.state.outbox = Outbox(
                    batch_id=str(
                        uuid5(
                            NAMESPACE_URL,
                            f"{self.integration_id}:{job.name}:{job.offset}",
                        )
                    ),
                    job_name=job.name,
                    events=events,
                    known_events=known,
                    next_offset=job.offset + len(response.records),
                    complete=complete,
                )
                await self.save()
                sent += await self.flush_outbox()
                pages += 1
        return {
            "events_sent": sent,
            "pages_read": pages,
            "jobs": [
                {
                    "prediction_id": j.prediction_id,
                    "status": j.status,
                    "offset": j.offset,
                    "error": j.error,
                }
                for j in self.state.jobs
            ],
        }

    def prepare_event(
        self, feature: Feature, job: Job, known: list[KnownEvent]
    ) -> Optional[dict]:
        if feature.id is None or feature.geometry is None:
            raise IntegrationBadResponseError(
                "A detection has no stable feature ID or polygon geometry."
            )
        geometry = feature.geometry.dict(exclude_none=True)
        try:
            polygon_geometry(geometry)
        except ValueError as exc:
            raise IntegrationBadResponseError(
                "Detection geometry is not a valid regional polygon."
            ) from exc
        props = feature.properties.model_properties()
        change_type = props.get("post_change")
        if change_type not in job.config.selected_types:
            return None
        changed = parse_change_date(props.get(job.config.change_date_property))
        if job.config.changes_since and changed < job.config.changes_since:
            return None
        geom = shape(geometry)
        external_id = str(
            uuid5(NAMESPACE_URL, f"{self.integration_id}:{job.result_id}:{feature.id}")
        )
        if any(event.external_id == external_id for event in known):
            return None
        matches = []
        for previous in known:
            if (
                previous.change_type != change_type
                or previous.last_prediction_id == job.prediction_id
            ):
                continue
            if (
                abs((previous.change_date - changed).days)
                > job.config.date_tolerance_days
            ):
                continue
            old_geom = shape(previous.geometry)
            if not geom.intersects(old_geom):
                continue
            union = geom.union(old_geom).area
            iou = geom.intersection(old_geom).area / union if union else 0
            if iou >= job.config.overlap_threshold:
                matches.append(previous)
        if len(matches) == 1:
            previous = matches[0]
            previous.last_seen_at = self.now
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
                first_seen_at=self.now,
                last_seen_at=self.now,
                last_prediction_id=job.prediction_id,
            )
        )
        point = geom.representative_point()
        props.update(
            {
                "prediction_id": job.prediction_id,
                "prediction_result_id": job.result_id,
                "oerun_prediction_result_id": feature.properties.oe_prediction_result_id,
                "external_event_id": external_id,
                "feature_id": feature.id,
                "model_id": str(job.config.model_id),
                "first_discovered_at": self.now.isoformat(),
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
            "source": f"olmoearth-monitor:{self.integration_id}",
            "recorded_at": datetime.combine(changed, time(), timezone.utc).isoformat(),
            "location": {"lat": point.y, "lon": point.x},
            "geometry": geometry,
            "event_details": props,
        }
