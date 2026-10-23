"""Orchestrate prediction submission, result collection, and recoverable delivery."""

import calendar
import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Awaitable, Callable
from uuid import NAMESPACE_URL, uuid5

from app.services.errors import (
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationError,
)
from app.services.gundi import valid_event_receipts

from .change_events import prepare_event
from .client import (
    FeatureSearchRequest,
    KeywordFilter,
    OlmoEarthClient,
    PredictionSearchRequest,
    PropertyFilter,
    StringFilter,
)
from .configurations import Cadence, PullEventsConfig
from .monitor_state import STATE_ACTION, Job, MonitorState, Outbox, monitor_identity

MAX_JOBS = 1000
RESULT_WINDOW = 10000


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
        identity = monitor_identity(self.config, self.api.base_url)
        self.state = (
            MonitorState.parse_obj(raw) if raw else MonitorState(identity=identity)
        )
        if self.state.identity != identity:
            raise IntegrationConfigurationError(
                "Environment, model, area, project, date semantics or schedule changed. Create a new monitor integration to preserve existing jobs and event identity."
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
        try:
            prediction = await self.api.create_prediction(body)
        except IntegrationError as exc:
            # Explicit pre-creation rejections are safe to retry on the next action.
            # Timeouts, 5xx, and malformed successful replies remain ambiguous.
            if exc.status_code in {400, 401, 403, 404, 422, 429}:
                self.state.jobs.remove(job)
                self.state.next_slot -= 1
                await self.save()
            raise
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
            if not valid_event_receipts(box.receipts, len(box.events)):
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
            if not valid_event_receipts(ids, len(box.events)):
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
                    event = prepare_event(
                        feature,
                        job,
                        known,
                        integration_id=self.integration_id,
                        now=self.now,
                    )
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
