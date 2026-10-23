"""Persisted monitor records and the identity that keeps configurations isolated."""

import hashlib
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field

from .configurations import PullEventsConfig

STATE_ACTION = "lcc_monitor_v1"


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
