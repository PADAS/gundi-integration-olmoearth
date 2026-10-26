"""Persisted monitor records and the identity that keeps configurations isolated."""

import hashlib
from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, root_validator

from .configurations import PullEventsConfig

STATE_ACTION = "lcc_monitor_v1"


class JobStatus(str, Enum):
    SUBMITTING = "submitting"  # POST sent or about to be; outcome unknown
    PENDING = "pending"
    COLLECTING = "collecting"
    COMPLETE = "complete"
    FAILED = "failed"
    NEEDS_EXPORT = "needs_export"


# Jobs that still have work to do; a new submission waits for these.
ACTIVE_JOB_STATUSES = frozenset({JobStatus.PENDING, JobStatus.COLLECTING})


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    SENDING = "sending"  # POST may have reached Gundi; never resent automatically
    ACCEPTED = "accepted"


class Job(BaseModel):
    name: str
    reference_time: str
    config: PullEventsConfig
    prediction_id: Optional[str] = None
    result_id: Optional[str] = None
    status: JobStatus = JobStatus.SUBMITTING
    submitted_at: Optional[datetime] = None
    offset: int = 0
    error: Optional[str] = None

    @root_validator(pre=True)
    def reference_time_from_request(cls, values):
        # Jobs saved before reference_time existed kept the whole request body.
        request = values.pop("request", None)
        if request and "reference_time" not in values:
            values["reference_time"] = request["start_time"]
        return values


class KnownEvent(BaseModel):
    external_id: str
    geometry: dict
    change_type: str
    change_date: date
    first_seen_at: datetime
    last_seen_at: datetime
    first_prediction_id: str
    last_prediction_id: str
    observations: int = 1
    gundi_object_id: Optional[str] = None

    @root_validator(pre=True)
    def default_first_prediction(cls, values):
        # Records saved before first_prediction_id existed.
        values.setdefault("first_prediction_id", values.get("last_prediction_id"))
        return values


class Outbox(BaseModel):
    """One page's delivery and the event-history changes it commits.

    The history changes are applied only once Gundi has accepted ``events``,
    together with the job's new offset, in a single store transaction.
    """

    batch_id: str
    delivery_status: DeliveryStatus = DeliveryStatus.PENDING
    receipts: list[str] = Field(default_factory=list)
    job_name: str
    events: list[dict]
    new_events: list[KnownEvent] = Field(default_factory=list)
    updated_events: list[KnownEvent] = Field(default_factory=list)
    next_offset: int
    complete: bool


class MonitorState(BaseModel):
    """The ledger: everything except the event history, which the store keeps
    per event so a page reads and writes only the events it touches."""

    identity: str
    next_slot: int = 0
    jobs: list[Job] = Field(default_factory=list)
    outbox: Optional[Outbox] = None


def monitor_identity(config: PullEventsConfig, base_url: str) -> str:
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
        (
            base_url.rstrip("/") + "\n" + config.json(include=fields, sort_keys=True)
        ).encode()
    ).hexdigest()
