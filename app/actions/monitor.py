"""Orchestrate prediction submission, result collection, and recoverable delivery."""

import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional
from uuid import NAMESPACE_URL, uuid5

from app.services.errors import (
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationError,
)
from app.services.state import IntegrationStateManager

from .change_events import EventStage, InvalidFeatureError, classify, prepare_event
from .client import (
    FeatureSearchRequest,
    KeywordFilter,
    OlmoEarthClient,
    Prediction,
    PredictionSearchRequest,
    PropertyFilter,
    StringFilter,
)
from .configurations import Cadence, PullEventsConfig
from .delivery import GundiEventSender
from .monitor_state import (
    ACTIVE_JOB_STATUSES,
    STATE_ACTION,
    DeliveryStatus,
    Job,
    JobStatus,
    MonitorState,
    Outbox,
    monitor_identity,
)

MAX_JOBS = 1000
# Elasticsearch's from+size limit for the features search.
RESULT_WINDOW = 10000
FEATURE_PAGE_SIZE = 100
PAGES_PER_POLL = 5
# Each prediction's reference date precedes its schedule slot by this much, so the
# imagery for the slot exists; verify against the chosen model.
IMAGERY_LAG_DAYS = 14


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


def build_feature_query(
    config: PullEventsConfig, job: Job, offset: int
) -> FeatureSearchRequest:
    return FeatureSearchRequest(
        limit=min(FEATURE_PAGE_SIZE, RESULT_WINDOW - offset),
        offset=offset,
        intersects_geometry=config.area_of_interest,
        property_filters=[
            PropertyFilter(
                property_name="post_change",
                keyword_filter=KeywordFilter(inc=job.selected_types),
            )
        ],
    )


class Monitor:
    def __init__(
        self,
        integration_id: str,
        config: PullEventsConfig,
        api: OlmoEarthClient,
        state_manager: IntegrationStateManager,
        sender: GundiEventSender,
        now: datetime,
    ):
        self.integration_id = integration_id
        self.config = config
        self.api = api
        self.store = state_manager
        self.sender = sender
        self.now = now.astimezone(timezone.utc)
        self.state: MonitorState

    async def load(self):
        raw = await self.store.get_state(self.integration_id, STATE_ACTION)
        identity = monitor_identity(self.config, self.api.base_url)
        self.state = (
            MonitorState.parse_obj(raw) if raw else MonitorState(identity=identity)
        )
        changed = sorted(
            key
            for key in identity.keys() | self.state.identity.keys()
            if identity.get(key) != self.state.identity.get(key)
        )
        if changed:
            raise IntegrationConfigurationError(
                f"Monitor settings changed ({', '.join(changed)}). Environment, model, area, "
                "project, date semantics or schedule changes need a new monitor integration "
                "to preserve existing jobs and event identity."
            )

    async def save(self):
        # One value holds the whole ledger, so every save is atomic.
        await self.store.set_state(self.integration_id, STATE_ACTION, self.state.dict())

    async def submit(self) -> dict:
        await self.load()
        if any(j.status in ACTIVE_JOB_STATUSES for j in self.state.jobs):
            return {"status": "waiting_for_existing_prediction"}
        slot = schedule_slot(self.config, self.state.next_slot)
        if slot > self.now.date():
            return {"status": "not_due", "next_prediction_date": slot.isoformat()}
        if len(self.state.jobs) >= MAX_JOBS:
            raise IntegrationConfigurationError(
                "Prototype job history is full; archive the monitor before submitting more runs."
            )
        project_id, model_id = str(self.config.project_id), str(self.config.model_id)
        if self.config.organization_id:
            organization = await self.api.project_organization(project_id)
            if organization != str(self.config.organization_id):
                raise IntegrationConfigurationError(
                    "The selected project does not belong to the configured organization."
                )
        reference = datetime.combine(
            slot - timedelta(days=IMAGERY_LAG_DAYS), time(), timezone.utc
        ).isoformat()
        # The name is unique per monitor and slot, so a POST whose outcome was lost
        # is found here on the next run instead of being sent twice.
        name = f"gundi:{self.integration_id}:{slot.isoformat()}"
        prediction = await self._find_prediction(name, project_id, model_id)
        recovered = prediction is not None
        if not recovered:
            prediction = await self.api.create_prediction(
                {
                    "name": name,
                    "project_id": project_id,
                    "model_id": model_id,
                    "start_time": reference,
                    "end_time": reference,
                    "geojson": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": self.config.area_of_interest,
                                "properties": {
                                    "start_time": reference,
                                    "end_time": reference,
                                },
                            }
                        ],
                    },
                }
            )
        self.state.jobs.append(
            Job(
                name=name,
                prediction_id=prediction.id,
                reference_time=reference,
                selected_types=self.config.selected_types,
                changes_since=self.config.changes_since,
            )
        )
        self.state.next_slot += 1
        await self.save()
        key = "recovered_prediction_id" if recovered else "submitted_prediction_id"
        return {key: prediction.id, "reference_date": reference}

    async def _find_prediction(
        self, name: str, project_id: str, model_id: str
    ) -> Optional[Prediction]:
        response = await self.api.search_predictions(
            PredictionSearchRequest(
                project_id=KeywordFilter(eq=project_id),
                model_id=KeywordFilter(eq=model_id),
                name=StringFilter(eq=name),
                limit=2,
            )
        )
        found = response.meta.total
        if found is None:
            found = len(response.records)
        if found > 1:
            raise IntegrationConfigurationError(
                f"{found} predictions are named {name}. Delete all but one in OlmoEarth "
                "before the monitor can continue."
            )
        return response.records[0] if response.records else None

    async def flush_outbox(self) -> int:
        box = self.state.outbox
        if box is None:
            return 0
        if box.delivery_status == DeliveryStatus.SENDING:
            raise IntegrationConfigurationError(
                f"Delivery outcome is unknown for batch {box.batch_id}. Use Resolve LCC Delivery after checking Gundi; it will not be resent automatically."
            )
        if box.events and box.delivery_status == DeliveryStatus.PENDING:
            # Resolve the Gundi client first: a failure here provably sent nothing,
            # so the batch stays pending and goes out on the next run.
            await self.sender.prepare()
            box.delivery_status = DeliveryStatus.SENDING
            await self.save()
            await self.sender.post(box.events)
            box.delivery_status = DeliveryStatus.ACCEPTED
            await self.save()
        updated = {event.external_id: event for event in box.updated_events}
        self.state.known_events = [
            updated.get(event.external_id, event) for event in self.state.known_events
        ] + box.new_events
        job = next(j for j in self.state.jobs if j.name == box.job_name)
        job.offset = box.next_offset
        job.status = JobStatus.COMPLETE if box.complete else JobStatus.COLLECTING
        self.state.outbox = None
        await self.save()
        return len(box.events)

    async def resolve_delivery(self, resolution) -> dict:
        await self.load()
        box = self.state.outbox
        if (
            box is None
            or box.batch_id != str(resolution.batch_id)
            or box.delivery_status != DeliveryStatus.SENDING
        ):
            raise IntegrationConfigurationError(
                "That batch is not awaiting delivery reconciliation."
            )
        box.delivery_status = (
            DeliveryStatus.ACCEPTED
            if resolution.outcome == "confirmed_delivered"
            else DeliveryStatus.PENDING
        )
        await self.save()
        return {"batch_id": box.batch_id, "status": box.delivery_status.value}

    async def _stop_job(self, job: Job, status: JobStatus, error: str):
        job.status = status
        job.error = error
        await self.save()

    async def _fail_job(self, job: Job, error: IntegrationError):
        await self._stop_job(job, JobStatus.FAILED, str(error))
        raise error

    async def _ready_for_paging(self, job: Job) -> bool:
        """Advance the active job from its prediction; True once it has a result to page."""
        try:
            prediction = await self.api.get_prediction(job.prediction_id)
        except IntegrationBadResponseError as exc:
            if exc.status_code == 404:
                await self._fail_job(
                    job,
                    IntegrationBadResponseError(
                        f"Prediction {job.prediction_id} no longer exists in OlmoEarth.",
                        status_code=404,
                    ),
                )
            raise
        if prediction.status in {"failed", "cancelled"}:
            await self._stop_job(
                job, JobStatus.FAILED, prediction.error_message or prediction.status
            )
            return False
        if prediction.status != "completed":
            return False
        if prediction.result is None or "mvt" not in prediction.result.file_formats:
            await self._fail_job(
                job,
                IntegrationConfigurationError(
                    "Completed prediction has no vector result. Use an LCC model configured with vectorization."
                ),
            )
        if job.result_id is not None and job.result_id != prediction.result.id:
            await self._fail_job(
                job,
                IntegrationBadResponseError(
                    "Prediction result changed while paging; earlier pages came from a different result."
                ),
            )
        if job.result_id is None or job.status != JobStatus.COLLECTING:
            job.result_id = prediction.result.id
            job.status = JobStatus.COLLECTING
            await self.save()
        return True

    async def _collect_page(self, job: Job, shapes: dict) -> tuple[int, int]:
        """Read, stage and deliver one page; return (events sent, repeats recorded)."""
        if job.offset >= RESULT_WINDOW:
            await self._stop_job(
                job,
                JobStatus.NEEDS_EXPORT,
                "Result reaches the 10,000-feature search window; completeness requires an export or a smaller AOI.",
            )
            return 0, 0
        query = build_feature_query(self.config, job, job.offset)
        response = await self.api.search_features(job.result_id, query)
        if response.meta.total is not None and response.meta.total > RESULT_WINDOW:
            await self._stop_job(
                job,
                JobStatus.NEEDS_EXPORT,
                "Result exceeds the feature search window; use a smaller AOI or export.",
            )
            return 0, 0
        stage = EventStage(self.state.known_events)
        try:
            events = []
            for feature in response.records:
                found = classify(feature, job, self.config)
                if found and (
                    event := prepare_event(
                        feature,
                        found,
                        job,
                        self.config,
                        stage,
                        shapes,
                        integration_id=self.integration_id,
                        now=self.now,
                    )
                ):
                    events.append(event)
        except InvalidFeatureError as exc:
            # Never skip a detection silently: stop the job at this page.
            await self._stop_job(job, JobStatus.FAILED, f"At offset {job.offset}: {exc}")
            raise
        batch = f"{self.integration_id}:{job.name}:{job.offset}"
        self.state.outbox = Outbox(
            batch_id=str(uuid5(NAMESPACE_URL, batch)),
            job_name=job.name,
            events=events,
            new_events=stage.new,
            updated_events=list(stage.updated.values()),
            next_offset=job.offset + len(response.records),
            # A short page proves exhaustion. ES totals can be lower bounds at 10,000.
            complete=len(response.records) < query.limit,
        )
        if events:
            # Persist the payload before sending; a page with nothing to send is
            # committed directly by flush_outbox.
            await self.save()
        return await self.flush_outbox(), len(stage.updated)

    async def collect(self) -> dict:
        await self.load()
        sent = await self.flush_outbox()
        pages = repeats = 0
        # submit() never starts a prediction while another is active, so there is
        # at most one job to collect.
        job = next((j for j in self.state.jobs if j.status in ACTIVE_JOB_STATUSES), None)
        if job and await self._ready_for_paging(job):
            # Shapely geometries of known events, built at most once per run.
            shapes = {}
            while pages < PAGES_PER_POLL and job.status == JobStatus.COLLECTING:
                page_sent, page_repeats = await self._collect_page(job, shapes)
                sent += page_sent
                repeats += page_repeats
                pages += 1
        return {
            "events_sent": sent,
            "repeats_recorded": repeats,
            "pages_read": pages,
            "known_events": len(self.state.known_events),
            "jobs": [
                {
                    "prediction_id": j.prediction_id,
                    "status": j.status.value,
                    "offset": j.offset,
                    "error": j.error,
                }
                for j in self.state.jobs
            ],
        }
