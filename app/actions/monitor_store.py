"""Where a monitor keeps its ledger and its per-event detection history.

The ledger (jobs, schedule position, outbox) is one small JSON value. Event
history lives beside it, one hash field per event plus a per-change-type index
ordered by change date, so matching a page reads only the events whose date is
within tolerance and committing a page writes only the events it changed.
"""

import json
from datetime import date
from typing import Iterable, Optional, Protocol

import stamina

from app.services.retry_policies import REDIS_RETRY
from app.services.state import IntegrationStateManager

from .monitor_state import STATE_ACTION, KnownEvent


class MonitorStore(Protocol):
    async def load_ledger(self) -> Optional[dict]:
        """The saved ledger, or None for a monitor that has never saved one."""

    async def load_legacy(self) -> Optional[dict]:
        """State saved by the single-blob layout, if any, for one-time migration."""

    async def save_ledger(self, ledger: dict) -> None: ...

    async def known_ids(self, external_ids: Iterable[str]) -> set[str]:
        """Which of these events are already in the committed history."""

    async def candidates(
        self, change_type: str, start: date, end: date
    ) -> list[KnownEvent]:
        """Committed events of one change type whose change date is in [start, end]."""

    async def commit(self, ledger: dict, events: list[KnownEvent]) -> None:
        """Write these events (new or updated) and the ledger atomically."""


class RedisMonitorStore:
    def __init__(self, state_manager: IntegrationStateManager, integration_id: str):
        self._redis = state_manager.db_client
        self._key = lambda source: state_manager.key(integration_id, STATE_ACTION, source)
        self._ledger = self._key("ledger")
        self._events = self._key("events")
        self._legacy = state_manager.key(integration_id, STATE_ACTION)

    def _index(self, change_type: str) -> str:
        return self._key(f"by_date.{change_type}")

    async def _get_json(self, key: str) -> Optional[dict]:
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                raw = await self._redis.get(key)
        return json.loads(raw) if raw else None

    async def load_ledger(self) -> Optional[dict]:
        return await self._get_json(self._ledger)

    async def load_legacy(self) -> Optional[dict]:
        return await self._get_json(self._legacy)

    async def save_ledger(self, ledger: dict) -> None:
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                await self._redis.set(self._ledger, json.dumps(ledger))

    async def known_ids(self, external_ids: Iterable[str]) -> set[str]:
        ids = list(external_ids)
        if not ids:
            return set()
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                async with self._redis.pipeline(transaction=False) as pipe:
                    for external_id in ids:
                        pipe.hexists(self._events, external_id)
                    present = await pipe.execute()
        return {external_id for external_id, hit in zip(ids, present) if hit}

    async def candidates(
        self, change_type: str, start: date, end: date
    ) -> list[KnownEvent]:
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                ids = await self._redis.zrangebyscore(
                    self._index(change_type), start.toordinal(), end.toordinal()
                )
                raw = await self._redis.hmget(self._events, ids) if ids else []
        return [KnownEvent.parse_raw(value) for value in raw if value is not None]

    async def commit(self, ledger: dict, events: list[KnownEvent]) -> None:
        # Every write is an idempotent overwrite, so retrying a transaction whose
        # reply was lost cannot apply a change twice.
        async for attempt in stamina.retry_context(**REDIS_RETRY):
            with attempt:
                async with self._redis.pipeline(transaction=True) as pipe:
                    if events:
                        pipe.hset(
                            self._events,
                            mapping={event.external_id: event.json() for event in events},
                        )
                    for event in events:
                        pipe.zadd(
                            self._index(event.change_type),
                            {event.external_id: event.change_date.toordinal()},
                        )
                    pipe.set(self._ledger, json.dumps(ledger))
                    await pipe.execute()
