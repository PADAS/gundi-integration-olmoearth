"""The monitor's persisted ledger and the identity that keeps configurations isolated."""

import json
from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from .configurations import PullEventsConfig

STATE_ACTION = "lcc_monitor_v1"


class JobStatus(str, Enum):
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
    """One submitted prediction and how far its result has been read.

    Only the settings that may change between jobs are kept; the rest are pinned
    by the monitor identity and read from the live configuration.
    """

    name: str
    prediction_id: str
    reference_time: str
    selected_types: list[str]
    changes_since: Optional[date] = None
    result_id: Optional[str] = None
    status: JobStatus = JobStatus.PENDING
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


class Outbox(BaseModel):
    """One page's delivery and the event-history changes it commits.

    The history changes are applied only once Gundi has accepted ``events``,
    in the same ledger write that advances the job's offset.
    """

    batch_id: str
    delivery_status: DeliveryStatus = DeliveryStatus.PENDING
    job_name: str
    events: list[dict]
    new_events: list[KnownEvent] = Field(default_factory=list)
    updated_events: list[KnownEvent] = Field(default_factory=list)
    next_offset: int
    complete: bool


class MonitorState(BaseModel):
    identity: dict
    next_slot: int = 0
    jobs: list[Job] = Field(default_factory=list)
    known_events: list[KnownEvent] = Field(default_factory=list)
    outbox: Optional[Outbox] = None


IDENTITY_FIELDS = {
    "project_id",
    "organization_id",
    "model_id",
    "area_of_interest",
    "schedule_start",
    "cadence",
    "change_date_property",
    "change_date_aggregation",
    "change_date_precision",
    "event_type",
}


def monitor_identity(config: PullEventsConfig, base_url: str) -> dict:
    """The settings a monitor's jobs and event IDs depend on; changing any needs a new monitor."""
    return {
        "base_url": base_url.rstrip("/"),
        **json.loads(config.json(include=IDENTITY_FIELDS)),
    }
