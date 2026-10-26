"""Orchestrate prediction submission, result collection, and recoverable delivery."""

import asyncio
import calendar
from datetime import date, datetime, time, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

from app.services.errors import (
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationError,
)

from .change_events import (
    EventStage,
    InvalidFeatureError,
    candidate_windows,
    classify,
    event_id,
    prepare_event,
)
from .client import (
    FeatureSearchRequest,
    KeywordFilter,
    OlmoEarthClient,
    PredictionSearchRequest,
    PropertyFilter,
    StringFilter,
    definitely_not_created,
)
from .configurations import Cadence, PullEventsConfig
from .delivery import GundiEventSender, valid_event_receipts
from .monitor_state import (
    ACTIVE_JOB_STATUSES,
    DeliveryStatus,
    Job,
    JobStatus,
    MonitorState,
    Outbox,
    monitor_identity,
)
from .monitor_store import MonitorStore

MAX_JOBS = 1000
RESULT_WINDOW = 10000
# How long a submission with an unknown outcome must stay unfindable by name
# before it is treated as never created and its schedule slot is reopened.
UNFOUND_SUBMISSION_GRACE = timedelta(hours=1)


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
        limit=min(config.feature_page_size, RESULT_WINDOW - offset),
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
        store: MonitorStore,
        sender: GundiEventSender,
        now: datetime,
    ):
        self.integration_id = integration_id
        self.config = config
        self.api = api
        self.store = store
        self.sender = sender
        self.now = now.astimezone(timezone.utc)
        self.state: MonitorState

    async def load(self):
        raw = await self.store.load_ledger()
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
        await self.store.save_ledger(self.state.dict())

    async def _release_slot(self, job: Job):
        """Forget a submission Studio never created, so its slot is submitted again."""
        self.state.jobs.remove(job)
        self.state.next_slot -= 1
        await self.save()

    async def submit(self) -> dict:
        await self.load()
        project_id, model_id = str(self.config.project_id), str(self.config.model_id)
        uncertain = next(
            (j for j in self.state.jobs if j.status == JobStatus.SUBMITTING), None
        )
        if uncertain:
            return await self._reconcile_submission(uncertain, project_id, model_id)
        if any(j.status in ACTIVE_JOB_STATUSES for j in self.state.jobs):
            return {"status": "waiting_for_existing_prediction"}
        slot = schedule_slot(self.config, self.state.next_slot)
        if slot > self.now.date():
            return {"status": "not_due", "next_prediction_date": slot.isoformat()}
        if len(self.state.jobs) >= MAX_JOBS:
            raise IntegrationConfigurationError(
                "Prototype job history is full; archive the monitor before submitting more runs."
            )
        if self.config.organization_id:
            organization = await self.api.project_organization(project_id)
            if organization != str(self.config.organization_id):
                raise IntegrationConfigurationError(
                    "The selected project does not belong to the configured organization."
                )
        reference = datetime.combine(
            slot - timedelta(days=self.config.imagery_lag_days), time(), timezone.utc
        ).isoformat()
        name = f"gundi:{self.integration_id}:{slot.isoformat()}"
        body = {
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
                        "properties": {"start_time": reference, "end_time": reference},
                    }
                ],
            },
        }
        job = Job(
            name=name,
            reference_time=reference,
            config=self.config.copy(deep=True),
            submitted_at=self.now,
        )
        self.state.jobs.append(job)
        self.state.next_slot += 1
        await self.save()
        try:
            prediction = await self.api.create_prediction(body)
        except IntegrationError as exc:
            if definitely_not_created(exc):
                await self._release_slot(job)
            raise
        job.prediction_id = prediction.id
        job.status = JobStatus.PENDING
        await self.save()
        return {"submitted_prediction_id": prediction.id, "reference_date": reference}

    async def _reconcile_submission(
        self, job: Job, project_id: str, model_id: str
    ) -> dict:
        # Search names include this integration and schedule slot; never retry the POST.
        response = await self.api.search_predictions(
            PredictionSearchRequest(
                project_id=KeywordFilter(eq=project_id),
                model_id=KeywordFilter(eq=model_id),
                name=StringFilter(eq=job.name),
                limit=2,
            )
        )
        found = response.meta.total
        if found is None:
            found = len(response.records)
        if found == 1 and len(response.records) == 1:
            job.prediction_id = response.records[0].id
            job.status = JobStatus.PENDING
            await self.save()
            return {"recovered_prediction_id": job.prediction_id}
        if found == 0 and self.now - job.submitted_at >= UNFOUND_SUBMISSION_GRACE:
            await self._release_slot(job)
            return {"status": "unsent_submission_released", "name": job.name}
        raise IntegrationConfigurationError(
            f"A prediction submission has an unknown outcome ({found} predictions named {job.name}). "
            "If none appears within an hour it is resubmitted automatically; if there are "
            "duplicates, delete all but one before the monitor can continue."
        )

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
            box.receipts = await self.sender.post(box.events)
            if not valid_event_receipts(box.receipts, len(box.events)):
                raise IntegrationBadResponseError(
                    "Gundi returned missing, invalid or duplicate object IDs; reconcile the outbox before retrying."
                )
            box.delivery_status = DeliveryStatus.ACCEPTED
            await self.save()
        by_id = {record.external_id: record for record in box.new_events}
        for event, object_id in zip(box.events, box.receipts):
            by_id[event["event_details"]["external_event_id"]].gundi_object_id = object_id
        job = next(j for j in self.state.jobs if j.name == box.job_name)
        job.offset = box.next_offset
        job.status = JobStatus.COMPLETE if box.complete else JobStatus.COLLECTING
        self.state.outbox = None
        await self.store.commit(self.state.dict(), box.new_events + box.updated_events)
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
        if resolution.outcome == "confirmed_delivered":
            ids = resolution.gundi_object_ids
            if not valid_event_receipts(ids, len(box.events)):
                raise IntegrationConfigurationError(
                    "Provide one distinct Gundi object ID for every outbox event, in order."
                )
            box.receipts = ids
            box.delivery_status = DeliveryStatus.ACCEPTED
        else:
            if resolution.gundi_object_ids:
                raise IntegrationConfigurationError(
                    "A confirmed undelivered batch must not have Gundi object IDs."
                )
            box.delivery_status = DeliveryStatus.PENDING
        await self.save()
        return {"batch_id": box.batch_id, "status": box.delivery_status.value}

    async def _stage(self, job: Job, classified: list) -> EventStage:
        """Fetch only the committed history this page's detections can touch."""
        windows = candidate_windows([found for _, found in classified], job)
        *found, known_ids = await asyncio.gather(
            *(
                self.store.candidates(change_type, start, end)
                for change_type, (start, end) in windows.items()
            ),
            self.store.known_ids(
                event_id(self.integration_id, job, feature)
                for feature, _ in classified
                if feature.id is not None
            ),
        )
        return EventStage([event for events in found for event in events], known_ids)

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

    async def _collect_page(self, job: Job, shapes: dict) -> int:
        """Read, stage and deliver one page of the collecting job; return events sent."""
        if job.offset >= RESULT_WINDOW:
            await self._stop_job(
                job,
                JobStatus.NEEDS_EXPORT,
                "Result reaches the 10,000-feature search window; completeness requires an export or a smaller AOI.",
            )
            return 0
        query = build_feature_query(job.config, job.offset)
        response = await self.api.search_features(job.result_id, query)
        if response.meta.total is not None and response.meta.total > RESULT_WINDOW:
            await self._stop_job(
                job,
                JobStatus.NEEDS_EXPORT,
                "Result exceeds the feature search window; use a smaller AOI or export.",
            )
            return 0
        try:
            classified = [
                (feature, found)
                for feature in response.records
                if (found := classify(feature, job))
            ]
            stage = await self._stage(job, classified)
            events = [
                event
                for feature, found in classified
                if (
                    event := prepare_event(
                        feature,
                        found,
                        job,
                        stage,
                        shapes,
                        integration_id=self.integration_id,
                        now=self.now,
                    )
                )
            ]
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
        return await self.flush_outbox()

    async def collect(self) -> dict:
        await self.load()
        sent = await self.flush_outbox()
        pages = 0
        # submit() never starts a prediction while another is active, so there is
        # at most one job to collect.
        job = next((j for j in self.state.jobs if j.status in ACTIVE_JOB_STATUSES), None)
        if job and await self._ready_for_paging(job):
            # Shapely geometries of known events, built at most once per run.
            shapes = {}
            while pages < self.config.pages_per_poll and job.status == JobStatus.COLLECTING:
                sent += await self._collect_page(job, shapes)
                pages += 1
        return {
            "events_sent": sent,
            "pages_read": pages,
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
