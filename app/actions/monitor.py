"""Orchestrate prediction submission, result collection, and recoverable delivery."""

import asyncio
import calendar
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

from app.services.activity_logger import LogLevel, log_action_activity
from app.services.errors import (
    IntegrationAuthError,
    IntegrationConnectionError,
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
from .imagery import select_thumbnail
from .monitor_state import (
    ACTIVE_JOB_STATUSES,
    STATE_ACTION,
    Job,
    JobStatus,
    MonitorState,
    Outbox,
    ThumbnailDelivery,
    monitor_identity,
    monitor_schedule,
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
        intersects_geometry=job.area_of_interest,
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
        # Only the identity keys still pinned are compared; older state also pinned
        # the area and schedule, which can now change.
        changed = sorted(
            key for key in identity if identity[key] != self.state.identity.get(key)
        )
        if changed:
            raise IntegrationConfigurationError(
                f"Monitor settings changed ({', '.join(changed)}). Environment, project "
                "or model changes need a new monitor integration to preserve existing "
                "jobs and event identity."
            )
        schedule = monitor_schedule(self.config)
        if raw and self.state.schedule is None:
            # State saved before the area and schedule could change pinned them in
            # the identity; move them to where they live now.
            self.state.schedule = {key: self.state.identity.get(key) for key in schedule}
            for job in self.state.jobs:
                job.area_of_interest = job.area_of_interest or self.state.identity.get(
                    "area_of_interest", self.config.area_of_interest
                )
        self.state.identity = identity
        if self.state.schedule != schedule:
            self._restart_schedule(schedule)

    def _restart_schedule(self, schedule: dict):
        """Count a new or changed schedule from its first date after both yesterday and
        the last submission, so it never back-fills or sends twice in a day. Kept in
        memory; the next submission saves it."""
        after = self.now.date() - timedelta(days=1)
        if self.state.jobs:
            after = max(after, self.state.jobs[-1].submitted_at.date())
        index = 0
        while schedule_slot(self.config, index) <= after:
            index += 1
        self.state.schedule = schedule
        self.state.next_slot = index

    def _due_slot(self) -> tuple[date, str, bool]:
        """The next prediction date, its name label, and whether it is an extra one.
        A due extra date goes first and leaves the schedule where it was."""
        extra = self.config.extra_prediction_on
        if extra and extra <= self.now.date():
            label = f"extra-{extra.isoformat()}"
            if not any(job.name == self._prediction_name(label) for job in self.state.jobs):
                return extra, label, True
        slot = schedule_slot(self.config, self.state.next_slot)
        return slot, slot.isoformat(), False

    def _prediction_name(self, label: str) -> str:
        # Unique per monitor and slot, so a POST whose outcome was lost is found by
        # name on the next run instead of being sent twice.
        return f"gundi:{self.integration_id}:{label}"

    async def save(self):
        # One value holds the whole ledger, so every save is atomic.
        await self.store.set_state(self.integration_id, STATE_ACTION, self.state.dict())

    async def submit(self) -> dict:
        await self.load()
        if any(j.status in ACTIVE_JOB_STATUSES for j in self.state.jobs):
            return {"status": "waiting_for_existing_prediction"}
        slot, label, is_extra = self._due_slot()
        if slot > self.now.date():
            return {"status": "not_due", "next_prediction_date": slot.isoformat()}
        area = self.config.area_of_interest
        project_id, model_id = str(self.config.project_id), str(self.config.model_id)
        reference = datetime.combine(
            slot - timedelta(days=IMAGERY_LAG_DAYS), time(), timezone.utc
        ).isoformat()
        name = self._prediction_name(label)
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
                                "geometry": area,
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
                include_thumbnails=self.config.include_thumbnails,
                area_of_interest=area,
                changes_since=self.config.changes_since,
                viewer_config_id=self.config.viewer_config_id and str(self.config.viewer_config_id),
            )
        )
        if not is_extra:
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

    async def _queue_thumbnails(self, events: list[dict], acknowledgements: list[dict]) -> None:
        """Queue a before and an after attachment for each event Gundi gave an ID."""
        missing = 0
        for event, ack in zip(events, acknowledgements):
            event_id = ack.get("object_id")
            if not isinstance(event_id, str) or not event_id:
                missing += 1
                continue
            # Keep only what scene selection reads; the full event is already delivered.
            details = event["event_details"]
            source = {
                "geometry": event.get("geometry"),
                "event_details": {k: details[k] for k in ("ts_start", "ts_end") if k in details},
            }
            for side in ("before", "after"):
                self.state.thumbnails.append(
                    ThumbnailDelivery(event_id=event_id, event=source, side=side)
                )
        if missing:
            await log_action_activity(
                self.integration_id, COLLECT_ACTION,
                f"{missing} event(s) accepted without an object_id; "
                "satellite attachments unavailable for them.",
                level=LogLevel.WARNING,
            )

    async def flush_outbox(self) -> list[dict]:
        """Deliver and commit the pending page, if any; return the events it sent.

        Delivery is at least once: if the POST fails, or its response is lost, the
        page stays in the outbox and is sent again on the next run. Each event keeps
        its external_event_id, so a rare duplicate is recognizable.
        """
        box = self.state.outbox
        if box is None:
            return []
        job = next(j for j in self.state.jobs if j.name == box.job_name)
        if box.events:
            acknowledgements = await self.sender.send(box.events)
            if job.include_thumbnails:
                await self._queue_thumbnails(box.events, acknowledgements)
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

    async def _publish_to_viewer(self, job: Job, result_id: str) -> None:
        """Add the completed prediction and result to the job's viewer before delivery."""
        lock = self.store.db_client.lock(
            f"{STATE_ACTION}:viewer:{self.api.base_url}:{job.viewer_config_id}",
            timeout=180,
            blocking=True,
            blocking_timeout=10,
        )
        if not await lock.acquire():
            raise IntegrationConnectionError(
                "Another monitor is updating this viewer; retry collection."
            )
        try:
            await asyncio.wait_for(
                self.api.add_to_viewer(job.viewer_config_id, job.prediction_id, result_id),
                timeout=150,
            )
        except asyncio.TimeoutError as exc:
            raise IntegrationConnectionError(
                "Publishing the prediction to the viewer timed out; retry collection."
            ) from exc
        except IntegrationError as exc:
            # Permanent errors must release the active job so corrected config
            # can take effect on the next submission. Transient errors retry.
            if isinstance(exc, (IntegrationConfigurationError, IntegrationAuthError)) or (
                exc.status_code is not None
                and 400 <= exc.status_code < 500
                and exc.status_code not in {408, 429}
            ):
                await self._fail_job(job, exc)
            raise
        finally:
            await lock.release()

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
            if job.viewer_config_id:
                await self._publish_to_viewer(job, prediction.result.id)
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
                    viewer_base_url=self.api.base_url,
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

    async def _deliver_thumbnail(self, item: ThumbnailDelivery) -> None:
        if item.selection is None:
            item.selection = await select_thumbnail(self.api, item.event, item.side, self.now)
            # Persist the chosen scene so a retry renders the same one.
            await self.save()
        if item.selection is None:
            await log_action_activity(
                self.integration_id, COLLECT_ACTION,
                f"No suitable {item.side} satellite scene for event {item.event_id}.",
                level=LogLevel.WARNING,
            )
            return
        data = await self.api.render_thumbnail(item.selection["query"])
        await self.sender.attach(item.event_id, item.selection["filename"], data)

    async def deliver_thumbnails(self):
        # Bound work per poll. Persist each completed side independently of events.
        for item in self.state.thumbnails[:4]:
            retry = False
            try:
                await asyncio.wait_for(self._deliver_thumbnail(item), timeout=90)
            except Exception:
                item.attempts += 1
                retry = item.attempts < 3
                await log_action_activity(
                    self.integration_id, COLLECT_ACTION,
                    f"Satellite attachment {item.side} for event {item.event_id} failed "
                    f"(attempt {item.attempts}/3)." +
                    (" Giving up; event remains delivered." if item.attempts >= 3 else " Will retry."),
                    level=LogLevel.WARNING,
                )
            # Remove only now: _deliver_thumbnail saves state while the item is queued.
            self.state.thumbnails.remove(item)
            if retry:
                self.state.thumbnails.append(item)
            await self.save()

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
        await self.deliver_thumbnails()
        kinds = Counter(event["event_details"]["detection_kind"] for event in sent)
        return {
            "events_sent": len(sent),
            "thumbnails_pending": len(self.state.thumbnails),
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
