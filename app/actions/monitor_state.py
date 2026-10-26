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
    request: dict
    config: PullEventsConfig
    prediction_id: Optional[str] = None
    result_id: Optional[str] = None
    status: JobStatus = JobStatus.SUBMITTING
    submitted_at: Optional[datetime] = None
    offset: int = 0
    error: Optional[str] = None


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
    batch_id: str
    delivery_status: DeliveryStatus = DeliveryStatus.PENDING
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
