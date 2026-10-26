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
from app.services.gundi import EventsNotSentError, valid_event_receipts

from .change_events import InvalidFeatureError, prepare_event
from .client import (
    FeatureSearchRequest,
    KeywordFilter,
    OlmoEarthClient,
    PredictionSearchRequest,
    PropertyFilter,
    RequestNotSentError,
    StringFilter,
)
from .configurations import Cadence, PullEventsConfig
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
RESULT_WINDOW = 10000
# Statuses that mean Studio rejected the POST before creating anything.
REJECTED_SUBMISSION_STATUSES = {400, 401, 403, 404, 422, 429}
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

    async def _release_slot(self, job: Job):
        """Forget a submission Studio never created, so its slot is submitted again."""
        self.state.jobs.remove(job)
        self.state.next_slot -= 1
        await self.save()

    async def submit(self) -> dict:
        await self.load()
        project_id, model_id = str(self.config.project_id), str(self.config.model_id)
        if self.config.organization_id:
            organization = await self.api.project_organization(project_id)
            if organization != str(self.config.organization_id):
                raise IntegrationConfigurationError(
                    "The selected project does not belong to the configured organization."
                )
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
            request=body,
            config=self.config.copy(deep=True),
            submitted_at=self.now,
        )
        self.state.jobs.append(job)
        self.state.next_slot += 1
        await self.save()
        try:
            prediction = await self.api.create_prediction(body)
        except IntegrationError as exc:
            # Explicit rejections and failed connections never created anything and
            # are safe to retry. Timeouts, 5xx and malformed replies stay ambiguous.
            if (
                isinstance(exc, RequestNotSentError)
                or exc.status_code in REJECTED_SUBMISSION_STATUSES
            ):
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
        if found == 0 and (
            job.submitted_at is None
            or self.now - job.submitted_at >= UNFOUND_SUBMISSION_GRACE
        ):
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
            box.delivery_status = DeliveryStatus.SENDING
            await self.save()
            try:
                box.receipts = await self.send(box.events)
            except EventsNotSentError:
                box.delivery_status = DeliveryStatus.PENDING
                await self.save()
                raise
            if not valid_event_receipts(box.receipts, len(box.events)):
                raise IntegrationBadResponseError(
                    "Gundi did not acknowledge every event in the outbox."
                )
            box.delivery_status = DeliveryStatus.ACCEPTED
            await self.save()
        by_id = {record.external_id: record for record in box.known_events}
        for event, object_id in zip(box.events, box.receipts):
            by_id[event["event_details"]["external_event_id"]].gundi_object_id = object_id
        self.state.known_events = box.known_events
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

    async def _stop_job(self, job: Job, status: JobStatus, error: str):
        job.status = status
        job.error = error
        await self.save()

    async def collect(self) -> dict:
        await self.load()
        await self.api.verify_token()
        sent = await self.flush_outbox()
        pages = 0
        # A bad result fails only its own job; the first such error is raised at the
        # end so the run is still reported as an error once the other jobs are done.
        job_errors: list[IntegrationError] = []
        # Shapely geometries of known events, built at most once per run.
        shapes = {}
        for job in self.state.jobs:
            if pages >= self.config.pages_per_poll:
                break
            if job.status not in ACTIVE_JOB_STATUSES:
                continue
            prediction = await self.api.get_prediction(job.prediction_id)
            if prediction.status in {"failed", "cancelled"}:
                await self._stop_job(
                    job, JobStatus.FAILED, prediction.error_message or prediction.status
                )
                continue
            if prediction.status != "completed":
                continue
            if prediction.result is None or "mvt" not in prediction.result.file_formats:
                error = IntegrationConfigurationError(
                    "Completed prediction has no vector result. Use an LCC model configured with vectorization."
                )
                await self._stop_job(job, JobStatus.FAILED, str(error))
                job_errors.append(error)
                continue
            if job.result_id is not None and job.result_id != prediction.result.id:
                error = IntegrationBadResponseError(
                    "Prediction result changed while paging; earlier pages came from a different result."
                )
                await self._stop_job(job, JobStatus.FAILED, str(error))
                job_errors.append(error)
                continue
            job.result_id = prediction.result.id
            job.status = JobStatus.COLLECTING
            await self.save()
            while pages < self.config.pages_per_poll and job.status == JobStatus.COLLECTING:
                if job.offset >= RESULT_WINDOW:
                    await self._stop_job(
                        job,
                        JobStatus.NEEDS_EXPORT,
                        "Result reaches the 10,000-feature search window; completeness requires an export or a smaller AOI.",
                    )
                    break
                query = build_feature_query(job.config, job.offset)
                response = await self.api.search_features(job.result_id, query)
                if (
                    response.meta.total is not None
                    and response.meta.total > RESULT_WINDOW
                ):
                    await self._stop_job(
                        job,
                        JobStatus.NEEDS_EXPORT,
                        "Result exceeds the feature search window; use a smaller AOI or export.",
                    )
                    break
                # A short page proves exhaustion. ES totals can be lower bounds at 10,000.
                complete = len(response.records) < query.limit
                known = [event.copy(deep=True) for event in self.state.known_events]
                try:
                    events = [
                        event
                        for feature in response.records
                        if (
                            event := prepare_event(
                                feature,
                                job,
                                known,
                                shapes,
                                integration_id=self.integration_id,
                                now=self.now,
                            )
                        )
                    ]
                except InvalidFeatureError as exc:
                    # Never skip a detection silently: stop this job at this page.
                    await self._stop_job(
                        job, JobStatus.FAILED, f"At offset {job.offset}: {exc}"
                    )
                    job_errors.append(exc)
                    break
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
        if job_errors:
            raise job_errors[0]
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
