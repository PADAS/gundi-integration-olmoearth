"""Orchestrate prediction submission, result collection, and recoverable delivery."""

import calendar
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

from app.services.activity_logger import LogLevel, log_action_activity
from app.services.errors import (
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationError,
)
from app.services.state import IntegrationStateManager

from .change_events import InvalidFeatureError, classify, prepare_event
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
    Job,
    JobStatus,
    MonitorState,
    Outbox,
    monitor_identity,
)

# Elasticsearch's from+size limit for the features search.
RESULT_WINDOW = 10000
FEATURE_PAGE_SIZE = 100
PAGES_PER_POLL = 5
# Each prediction's reference date precedes its schedule slot by this much, so the
# imagery for the slot exists; verify against the chosen model.
IMAGERY_LAG_DAYS = 14
# A prediction still unfinished this long after submission (stalled, or in a status
# the monitor does not know) fails its job, so the monitor moves on to the next slot.
MAX_PREDICTION_WAIT = timedelta(days=3)
# The action id that collection logs under in the portal.
COLLECT_ACTION = "pull_events"


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
        project_id, model_id = str(self.config.project_id), str(self.config.model_id)
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
                submitted_at=self.now,
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

    async def flush_outbox(self) -> list[dict]:
        """Deliver and commit the pending page, if any; return the events it sent.

        Delivery is at least once: if the POST fails, or its response is lost, the
        page stays in the outbox and is sent again on the next run. Each event keeps
        its external_event_id, so a rare duplicate is recognizable.
        """
        box = self.state.outbox
        if box is None:
            return []
        if box.events:
            await self.sender.send(box.events)
        job = next(j for j in self.state.jobs if j.name == box.job_name)
        job.offset = box.next_offset
        job.status = JobStatus.COMPLETE if box.complete else JobStatus.COLLECTING
        self.state.outbox = None
        await self.save()
        return box.events

    async def _stop_job(self, job: Job, status: JobStatus, error: str):
        job.status = status
        job.error = error
        await self.save()
        # A stopped job does not fail the run, so flag it where operators look.
        await log_action_activity(
            self.integration_id,
            COLLECT_ACTION,
            f"Prediction {job.prediction_id} stopped ({status.value}): {error}",
            level=LogLevel.WARNING,
            data={"job": job.name, "prediction_id": job.prediction_id, "status": status.value},
        )

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
            waited = self.now - job.submitted_at
            if waited > MAX_PREDICTION_WAIT:
                await self._stop_job(
                    job,
                    JobStatus.FAILED,
                    f"Prediction did not finish within {MAX_PREDICTION_WAIT.days} days "
                    f"(last status: {prediction.status}).",
                )
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

    async def _collect_page(self, job: Job) -> list[dict]:
        """Read, stage and deliver one page; return the events sent."""
        if job.offset >= RESULT_WINDOW:
            await self._stop_job(
                job,
                JobStatus.NEEDS_EXPORT,
                "Result reaches the 10,000-feature search window; completeness requires an export or a smaller AOI.",
            )
            return []
        query = build_feature_query(self.config, job, job.offset)
        response = await self.api.search_features(job.result_id, query)
        if response.meta.total is not None and response.meta.total > RESULT_WINDOW:
            await self._stop_job(
                job,
                JobStatus.NEEDS_EXPORT,
                "Result exceeds the feature search window; use a smaller AOI or export.",
            )
            return []
        # A working copy: an invalid page leaves the committed site records untouched.
        sites = {site: record.copy(deep=True) for site, record in self.state.sites.items()}
        try:
            events = [
                prepare_event(
                    feature,
                    found,
                    job,
                    self.config,
                    sites,
                    integration_id=self.integration_id,
                    now=self.now,
                )
                for feature in response.records
                if (found := classify(feature, job, self.config))
            ]
        except InvalidFeatureError as exc:
            # Never skip a detection silently: stop the job at this page.
            await self._stop_job(job, JobStatus.FAILED, f"At offset {job.offset}: {exc}")
            raise
        # The site records are saved with the outbox, which is resent until delivered
        # and never dropped, so they cannot run ahead of what is sent.
        self.state.sites = sites
        self.state.outbox = Outbox(
            job_name=job.name,
            events=events,
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
        sent = list(await self.flush_outbox())
        pages = 0
        # submit() never starts a prediction while another is active, so there is
        # at most one job to collect.
        job = next((j for j in self.state.jobs if j.status in ACTIVE_JOB_STATUSES), None)
        if job and await self._ready_for_paging(job):
            while pages < PAGES_PER_POLL and job.status == JobStatus.COLLECTING:
                sent += await self._collect_page(job)
                pages += 1
        kinds = Counter(event["event_details"]["detection_kind"] for event in sent)
        return {
            "events_sent": len(sent),
            "detections_by_kind": dict(kinds),
            "pages_read": pages,
            "known_sites": len(self.state.sites),
            # Only the job this run worked on, so log entries stay short as history grows.
            "job": job and {
                "prediction_id": job.prediction_id,
                "status": job.status.value,
                "offset": job.offset,
                "error": job.error,
            },
        }
