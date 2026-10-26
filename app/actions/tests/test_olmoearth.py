"""Contracts against Studio develop and recoverable LCC monitor workflows."""

import copy
import datetime as dt
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from pydantic import ValidationError

from app.actions import client, handlers
from app.actions.change_events import parse_change_date
from app.actions.configurations import (
    Cadence,
    PullEventsConfig,
    ResolveDeliveryConfig,
    SubmitPredictionsConfig,
)
from app.actions.monitor import (
    Monitor,
    build_feature_query,
    schedule_slot,
)
from app.actions.monitor_state import Job, KnownEvent
from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
)

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 23, tzinfo=UTC)
PROJECT = "00000000-0000-0000-0000-000000000001"
MODEL = "00000000-0000-0000-0000-000000000002"
ORG = "00000000-0000-0000-0000-000000000003"
AREA = {
    "type": "Polygon",
    "coordinates": [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]],
}


@pytest.fixture
def config():
    return PullEventsConfig(
        project_id=PROJECT,
        model_id=MODEL,
        area_of_interest=AREA,
        schedule_start="2026-10-01",
        cadence="weekly",
        select_all_types=False,
        post_change_types=["mining"],
        change_date_property="onset",
        change_date_aggregation="percentile_10",
        feature_page_size=2,
        pages_per_poll=1,
    )


class MemoryMonitorStore:
    """In-memory MonitorStore with the same contract as RedisMonitorStore."""

    def __init__(self):
        self.ledger = None
        self.events = {}

    async def load_ledger(self):
        return copy.deepcopy(self.ledger)

    async def save_ledger(self, ledger):
        self.ledger = copy.deepcopy(ledger)

    async def known_ids(self, external_ids):
        return {external_id for external_id in external_ids if external_id in self.events}

    async def candidates(self, change_type, start, end):
        return [
            KnownEvent.parse_obj(event)
            for event in self.events.values()
            if event["change_type"] == change_type
            and start <= dt.date.fromisoformat(event["change_date"]) <= end
        ]

    async def commit(self, ledger, events):
        for event in events:
            self.events[event.external_id] = json.loads(event.json())
        self.ledger = copy.deepcopy(ledger)

    @property
    def value(self):
        """The ledger plus committed events, for assertions."""
        return {**(self.ledger or {}), "known_events": list(self.events.values())}


@pytest.fixture
def store():
    return MemoryMonitorStore()


@pytest.fixture
def api():
    api = AsyncMock(spec=client.OlmoEarthClient)
    api.base_url = "https://example.test"
    api.verify_token.return_value = 0
    api.create_prediction.return_value = prediction("p1", status="pending")
    api.get_prediction.return_value = prediction("p1")
    api.search_features.return_value = page([])
    return api


def prediction(pid="p1", status="completed", result_id="r1", formats=None):
    return client.Prediction(
        id=pid,
        status=status,
        result=client.PredictionResult(
            id=result_id,
            file_formats=formats if formats is not None else ["mvt", "png"],
        ),
    )


def feature(fid="f1", changed="2026-09-10", change_type="mining", geometry=None):
    return client.Feature.parse_obj(
        {
            "id": fid,
            "type": "Feature",
            "geometry": geometry or AREA,
            "properties": {
                "post_change": change_type,
                "onset": changed,
                "oe_start_time": "2026-10-01T00:00:00Z",
                "oe_prediction_result_id": "run-result-id",
            },
        }
    )


def page(features, total=None):
    return client.FeatureSearchResponse(
        records=features, meta=client.SearchMeta(total=total)
    )


def make_monitor(config, api, store, now=NOW):
    async def accept(events):
        return [
            "gundi-" + event["event_details"]["external_event_id"] for event in events
        ]

    sender = SimpleNamespace(prepare=AsyncMock(), post=AsyncMock(side_effect=accept))
    return Monitor("integration-1", config, api, store, sender, now)


def pending_job(config, prediction_id="p1", name=None, reference_time="2026-10-01T00:00:00Z"):
    return Job(
        name=name or prediction_id,
        reference_time=reference_time,
        config=config,
        prediction_id=prediction_id,
        status="pending",
        submitted_at=NOW,
    )


async def seed(config, api, store, jobs=None):
    monitor = make_monitor(config, api, store)
    await monitor.load()
    monitor.state.jobs = jobs or [
        pending_job(config, "p1", name="job", reference_time="2026-10-01T00:00:00Z")
    ]
    await monitor.save()
    return monitor


def test_monitor_form_requires_selection_and_valid_polygon(config):
    assert SubmitPredictionsConfig().run_on_schedule is False
    assert config.selected_types == ["mining"]
    assert "none" not in config.copy(update={"select_all_types": True}).selected_types
    for changes in (
        {"post_change_types": []},
        {"project_id": "bad"},
        {"area_of_interest": {"type": "Point", "coordinates": [1, 2]}},
    ):
        with pytest.raises(ValidationError):
            PullEventsConfig.parse_obj({**config.dict(), **changes})


def test_calendar_schedule_keeps_anchor_after_short_month(config):
    config.cadence = Cadence.MONTHLY
    config.schedule_start = dt.date(2024, 1, 31)
    assert schedule_slot(config, 1) == dt.date(2024, 2, 29)
    assert schedule_slot(config, 2) == dt.date(2024, 3, 31)
    config.cadence = Cadence.ANNUAL
    config.schedule_start = dt.date(2024, 2, 29)
    assert schedule_slot(config, 1) == dt.date(2025, 2, 28)


def test_feature_filter_uses_post_change_and_not_a_moving_date_cursor(config):
    body = build_feature_query(config, 6).as_body()
    assert body["intersects_geometry"] == AREA
    assert body["offset"] == 6
    assert body["property_filters"] == [
        {"property_name": "post_change", "keyword_filter": {"inc": ["mining"]}}
    ]
    assert "oe_created_at" not in body and "oe_start_time" not in body


@pytest.mark.parametrize(
    "value", [None, 145, "21", "2026-99-99", "2026-01-01T12:00:00"]
)
def test_encoded_or_invalid_dates_are_not_silently_used(value):
    with pytest.raises(IntegrationBadResponseError):
        parse_change_date(value)


@pytest.mark.asyncio
async def test_due_submission_is_saved_with_reference_date_and_no_vectorize_flag(
    config, api, store
):
    result = await make_monitor(config, api, store).submit()
    body = api.create_prediction.call_args.args[0]
    assert body["project_id"] == PROJECT
    assert body["start_time"] == body["end_time"] == "2026-09-17T00:00:00+00:00"
    assert body["geojson"]["features"][0]["geometry"] == AREA
    assert "vectorize" not in body
    assert result["submitted_prediction_id"] == "p1"
    assert store.value["jobs"][0]["prediction_id"] == "p1"
    assert store.value["next_slot"] == 1
    await make_monitor(config, api, store).submit()
    api.create_prediction.assert_awaited_once()


@pytest.mark.asyncio
async def test_future_slot_does_not_submit(config, api, store):
    config.schedule_start = dt.date(2027, 1, 1)
    assert (await make_monitor(config, api, store).submit())["status"] == "not_due"
    api.create_prediction.assert_not_called()


@pytest.mark.asyncio
async def test_organization_mismatch_cannot_submit(config, api, store):
    config.organization_id = ORG
    api.project_organization.return_value = "different-org"
    with pytest.raises(IntegrationConfigurationError):
        await make_monitor(config, api, store).submit()
    api.create_prediction.assert_not_called()


@pytest.mark.asyncio
async def test_ambiguous_submission_recovers_by_name_without_duplicate_post(
    config, api, store
):
    api.create_prediction.side_effect = IntegrationConnectionError("timeout")
    with pytest.raises(IntegrationConnectionError):
        await make_monitor(config, api, store).submit()
    assert store.value["jobs"][0]["status"] == "submitting"
    api.search_predictions.return_value = client.PredictionSearchResponse(
        records=[prediction("recovered")]
    )
    result = await make_monitor(config, api, store).submit()
    assert result == {"recovered_prediction_id": "recovered"}
    assert (
        api.search_predictions.call_args.args[0].name.eq
        == store.value["jobs"][0]["name"]
    )
    api.create_prediction.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_submission_without_a_match_requires_reconciliation(
    config, api, store
):
    api.create_prediction.side_effect = IntegrationConnectionError("timeout")
    with pytest.raises(IntegrationConnectionError):
        await make_monitor(config, api, store).submit()
    api.search_predictions.return_value = client.PredictionSearchResponse(records=[])
    with pytest.raises(IntegrationConfigurationError, match="unknown outcome"):
        await make_monitor(config, api, store).submit()
    api.create_prediction.assert_awaited_once()


@pytest.mark.asyncio
async def test_late_discovery_uses_change_date_and_keeps_both_result_ids(
    config, api, store
):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    result = await monitor.collect()
    event = monitor.sender.post.call_args.args[0][0]
    assert event["recorded_at"].startswith("2026-09-10")
    assert event["geometry"] == AREA
    assert event["event_details"]["first_discovered_at"].startswith("2026-10-23")
    assert event["event_details"]["prediction_result_id"] == "r1"
    assert event["event_details"]["oerun_prediction_result_id"] == "run-result-id"
    assert result["jobs"][0]["status"] == "complete"
    assert api.search_features.call_args.args[0] == "r1"


@pytest.mark.asyncio
async def test_page_budget_resumes_without_marking_complete(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.side_effect = [
        page([feature("1"), feature("2")]),
        page([feature("3")]),
    ]
    await monitor.collect()
    assert store.value["jobs"][0]["offset"] == 2
    assert store.value["jobs"][0]["status"] == "collecting"
    await monitor.collect()
    assert api.search_features.call_args.args[1].offset == 2
    assert store.value["jobs"][0]["status"] == "complete"


@pytest.mark.asyncio
async def test_failed_delivery_replays_identical_outbox_then_advances(
    config, api, store
):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.post.side_effect = RuntimeError("delivery acknowledgement lost")
    with pytest.raises(RuntimeError):
        await monitor.collect()
    first = copy.deepcopy(monitor.sender.post.call_args.args[0])
    assert store.value["jobs"][0]["offset"] == 0
    assert store.value["outbox"] is not None
    with pytest.raises(IntegrationConfigurationError, match="unknown"):
        await monitor.collect()
    monitor.sender.post.assert_awaited_once()
    await monitor.resolve_delivery(
        ResolveDeliveryConfig(
            batch_id=store.value["outbox"]["batch_id"],
            outcome="confirmed_not_delivered",
        )
    )
    monitor.sender.post.side_effect = None
    monitor.sender.post.return_value = ["gundi-receipt-1"]
    await monitor.collect()
    assert monitor.sender.post.call_args.args[0] == first
    api.search_features.assert_awaited_once()
    assert store.value["jobs"][0]["status"] == "complete"
    assert store.value["outbox"] is None


@pytest.mark.asyncio
async def test_repeat_polygon_in_next_prediction_is_not_another_event(
    config, api, store
):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    await monitor.collect()
    monitor.state.jobs.append(
        pending_job(config, "p2", name="second", reference_time="2026-10-08T00:00:00Z")
    )
    await monitor.save()
    api.get_prediction.return_value = prediction("p2", result_id="r2")
    api.search_features.return_value = page(
        [feature("different-feature-id", changed="2026-09-12")]
    )
    result = await monitor.collect()
    assert result["events_sent"] == 0
    assert len(store.value["known_events"]) == 1
    assert store.value["known_events"][0]["observations"] == 2
    monitor.sender.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_distinct_date_creates_a_new_event(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    await monitor.collect()
    monitor.state.jobs.append(
        pending_job(config, "p2", name="second", reference_time="2026-10-08T00:00:00Z")
    )
    await monitor.save()
    api.get_prediction.return_value = prediction("p2", result_id="r2")
    api.search_features.return_value = page([feature("f2", changed="2026-02-01")])
    assert (await monitor.collect())["events_sent"] == 1


@pytest.mark.asyncio
async def test_failed_prediction_is_recorded_and_other_jobs_can_continue(
    config, api, store
):
    monitor = await seed(config, api, store)
    api.get_prediction.return_value = prediction(status="failed")
    result = await monitor.collect()
    assert result["jobs"][0]["status"] == "failed"
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_raster_only_result_is_not_treated_as_no_detections(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.return_value = prediction(formats=["png"])
    with pytest.raises(IntegrationConfigurationError, match="vector"):
        await monitor.collect()
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_event_does_not_advance_or_drop_a_page(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature(changed=145)])
    with pytest.raises(IntegrationBadResponseError):
        await monitor.collect()
    assert store.value["jobs"][0]["offset"] == 0
    assert store.value["jobs"][0]["status"] == "failed"
    assert "offset 0" in store.value["jobs"][0]["error"]
    monitor.sender.post.assert_not_called()


@pytest.mark.asyncio
async def test_search_window_is_explicitly_incomplete(config, api, store):
    monitor = await seed(config, api, store)
    monitor.state.jobs[0].offset = 10000
    await monitor.save()
    result = await monitor.collect()
    assert result["jobs"][0]["status"] == "needs_export"
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_config_changes_do_not_mix_monitor_identity(config, api, store):
    await seed(config, api, store)
    config.model_id = ORG
    with pytest.raises(IntegrationConfigurationError, match="new monitor"):
        await make_monitor(config, api, store).collect()


@pytest.mark.asyncio
async def test_auth_failure_prevents_optional_auth_feature_search(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.side_effect = IntegrationAuthError("expired")
    with pytest.raises(IntegrationAuthError):
        await monitor.collect()
    api.search_features.assert_not_called()


@pytest.mark.asyncio
async def test_actual_http_contracts_use_nested_result_and_per_result_search(config):
    seen = []

    def route(request):
        seen.append(request)
        if request.url.path.endswith("/features/search"):
            return httpx.Response(200, json={"records": [], "meta": {"total": 0}})
        return httpx.Response(
            200,
            json={
                "records": [
                    {
                        "id": "p1",
                        "status": "completed",
                        "result": {"id": "studio-result", "file_formats": ["mvt"]},
                    }
                ]
            },
        )

    async with client.OlmoEarthClient(
        "https://example.test", "token", transport=httpx.MockTransport(route)
    ) as http:
        pred = await http.get_prediction("p1")
        await http.search_features(pred.result.id, build_feature_query(config, 0))
    assert seen[0].method == "GET"
    assert (
        seen[1].url.path == "/api/v1/prediction-results/studio-result/features/search"
    )
    assert "prediction_results" not in json.loads(seen[1].content)
    assert seen[1].headers["Authorization"] == "Bearer token"


@pytest.mark.asyncio
async def test_http_creation_timeout_is_not_retried():
    requests = []

    def route(request):
        requests.append(request)
        raise httpx.ReadTimeout("response lost", request=request)

    async with client.OlmoEarthClient(
        "https://example.test", "token", transport=httpx.MockTransport(route)
    ) as http:
        with pytest.raises(IntegrationConnectionError):
            await http.create_prediction({"name": "unique-job"})
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"records": [], "errors": [{"message": "partial"}]},
        {"records": [{"id": "p1"}], "errors": [{"message": "partial"}]},
        {"records": "invalid"},
    ],
)
async def test_incomplete_api_responses_are_not_checkpointed(payload):
    async with client.OlmoEarthClient(
        "https://example.test",
        "token",
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=payload)),
    ) as http:
        with pytest.raises(IntegrationBadResponseError):
            await http.search_predictions(client.PredictionSearchRequest())


@pytest.mark.asyncio
async def test_lock_prevents_overlapping_actions(config, mocker):
    lock = SimpleNamespace(acquire=AsyncMock(return_value=False), release=AsyncMock())
    mocker.patch.object(handlers.state_manager.db_client, "lock", return_value=lock)
    api_factory = mocker.patch.object(handlers, "client_for")
    assert await handlers.run_monitor(
        SimpleNamespace(id="integration-1"), config, Monitor.submit
    ) == {"status": "already_running"}
    api_factory.assert_not_called()


@pytest.mark.asyncio
async def test_lock_released_after_failure(config, mocker):
    lock = SimpleNamespace(acquire=AsyncMock(return_value=True), release=AsyncMock())
    mocker.patch.object(handlers.state_manager.db_client, "lock", return_value=lock)
    mocker.patch.object(
        handlers, "client_for", side_effect=RuntimeError("test failure")
    )
    with pytest.raises(RuntimeError):
        await handlers.run_monitor(
            SimpleNamespace(id="integration-1"), config, Monitor.submit
        )
    lock.release.assert_awaited_once()


@pytest.mark.asyncio
async def test_confirmed_delivery_commits_receipts_without_reposting(
    config, api, store
):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.post.side_effect = RuntimeError("lost acknowledgement")
    with pytest.raises(RuntimeError):
        await monitor.collect()
    batch_id = store.value["outbox"]["batch_id"]
    await monitor.resolve_delivery(
        ResolveDeliveryConfig(
            batch_id=batch_id,
            outcome="confirmed_delivered",
            gundi_object_ids=["actual-gundi-object"],
        )
    )
    await monitor.collect()
    monitor.sender.post.assert_awaited_once()
    assert store.value["known_events"][0]["gundi_object_id"] == "actual-gundi-object"
    assert store.value["jobs"][0]["status"] == "complete"


@pytest.mark.asyncio
async def test_delivery_resolution_rejects_incomplete_receipts(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.post.side_effect = RuntimeError("unknown")
    with pytest.raises(RuntimeError):
        await monitor.collect()
    with pytest.raises(IntegrationConfigurationError, match="one distinct"):
        await monitor.resolve_delivery(
            ResolveDeliveryConfig(
                batch_id=store.value["outbox"]["batch_id"],
                outcome="confirmed_delivered",
            )
        )
    assert store.value["outbox"]["delivery_status"] == "sending"


@pytest.mark.asyncio
async def test_result_creation_failure_can_recover_later(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.return_value = prediction(status="result_creation_failed")
    await monitor.collect()
    assert store.value["jobs"][0]["status"] == "pending"
    api.get_prediction.return_value = prediction()
    await monitor.collect()
    assert store.value["jobs"][0]["status"] == "complete"


@pytest.mark.asyncio
async def test_changed_selection_only_applies_to_future_jobs(config, api, store):
    await seed(config, api, store)
    config.post_change_types = ["new_road"]
    api.search_features.return_value = page([feature()])
    result = await make_monitor(config, api, store).collect()
    assert result["events_sent"] == 1
    assert api.search_features.call_args.args[1].property_filters[
        0
    ].keyword_filter.inc == ["mining"]


@pytest.mark.asyncio
async def test_event_and_monitor_id_are_separate(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    await monitor.collect()
    event = monitor.sender.post.call_args.args[0][0]
    assert event["source"] == "olmoearth-monitor:integration-1"
    assert event["event_details"]["external_event_id"] != event["source"]


def test_portal_schema_and_example_keep_environment_and_ids_configurable():
    from pathlib import Path

    example = json.loads(
        (
            Path(__file__).resolve().parents[3] / "examples" / "lcc-monitor.json"
        ).read_text()
    )
    parsed = PullEventsConfig.parse_obj(example)
    schema = PullEventsConfig.schema()
    assert "project_id" in schema["required"] and "model_id" in schema["required"]
    assert "change_date_property" in schema["required"]
    assert schema["properties"]["post_change_types"]["type"] == "array"
    assert schema["properties"]["post_change_types"]["uniqueItems"] is True
    assert PullEventsConfig.ui_schema()["post_change_types"]["ui:widget"] == "select"
    assert parsed.selected_types == ["mining"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 429])
async def test_rejected_submission_preserves_slot_for_retry(config, api, store, status):
    api.create_prediction.side_effect = IntegrationAuthError(
        "Rejected", status_code=status
    )
    with pytest.raises(IntegrationAuthError):
        await make_monitor(config, api, store).submit()
    assert store.value["next_slot"] == 0
    assert store.value["jobs"] == []
    api.create_prediction.side_effect = None
    await make_monitor(config, api, store).submit()
    assert store.value["next_slot"] == 1
    assert store.value["jobs"][0]["prediction_id"] == "p1"
    api.search_predictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_environment_change_cannot_reuse_saved_jobs(config, api, store):
    await seed(config, api, store)
    api.base_url = "https://another-environment.test"
    with pytest.raises(IntegrationConfigurationError, match="Environment"):
        await make_monitor(config, api, store).collect()
    api.get_prediction.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("receipts", [[" ", "id2"], ["id1", "id1"]])
async def test_invalid_receipts_pause_outbox(config, api, store, receipts):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature("f1"), feature("f2")])
    monitor.sender.post = AsyncMock(return_value=receipts)
    with pytest.raises(IntegrationBadResponseError):
        await monitor.collect()
    assert store.value["outbox"]["delivery_status"] == "sending"
    assert store.value["jobs"][0]["offset"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,retryable_slot", [(401, True), (422, True), (503, False)]
)
async def test_submission_http_rejection_vs_ambiguous_failure(
    config, store, status, retryable_slot
):
    calls = []

    def route(request):
        calls.append(request)
        return httpx.Response(status, json={"detail": "failure"})

    async with client.OlmoEarthClient(
        "https://example.test", "token", transport=httpx.MockTransport(route)
    ) as api:
        from app.services.errors import IntegrationError

        with pytest.raises(IntegrationError):
            await make_monitor(config, api, store).submit()
    assert len(calls) == 1
    assert store.value["next_slot"] == (0 if retryable_slot else 1)
    assert bool(store.value["jobs"]) is not retryable_slot


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["needs_export", "failed"])
async def test_stopped_job_does_not_block_next_submission(config, api, store, status):
    monitor = await seed(config, api, store)
    monitor.state.jobs[0].status = status
    monitor.state.next_slot = 1
    await monitor.save()
    result = await make_monitor(config, api, store).submit()
    assert result["submitted_prediction_id"] == "p1"


@pytest.mark.asyncio
async def test_connection_failure_before_sending_releases_slot(config, api, store):
    api.create_prediction.side_effect = client.RequestNotSentError("refused")
    with pytest.raises(IntegrationConnectionError):
        await make_monitor(config, api, store).submit()
    assert store.value["jobs"] == [] and store.value["next_slot"] == 0


@pytest.mark.asyncio
async def test_unfound_submission_is_released_after_grace(config, api, store):
    api.create_prediction.side_effect = IntegrationConnectionError("timeout")
    with pytest.raises(IntegrationConnectionError):
        await make_monitor(config, api, store).submit()
    api.search_predictions.return_value = client.PredictionSearchResponse(records=[])
    later = NOW + dt.timedelta(hours=2)
    result = await make_monitor(config, api, store, now=later).submit()
    assert result["status"] == "unsent_submission_released"
    assert store.value["jobs"] == [] and store.value["next_slot"] == 0
    api.create_prediction.side_effect = None
    await make_monitor(config, api, store, now=later).submit()
    assert store.value["jobs"][0]["status"] == "pending"


@pytest.mark.asyncio
async def test_duplicate_submissions_are_never_released(config, api, store):
    api.create_prediction.side_effect = IntegrationConnectionError("timeout")
    with pytest.raises(IntegrationConnectionError):
        await make_monitor(config, api, store).submit()
    api.search_predictions.return_value = client.PredictionSearchResponse(
        records=[prediction("a"), prediction("b")], meta=client.SearchMeta(total=2)
    )
    later = NOW + dt.timedelta(hours=2)
    with pytest.raises(IntegrationConfigurationError, match="unknown outcome"):
        await make_monitor(config, api, store, now=later).submit()
    assert store.value["jobs"][0]["status"] == "submitting"


@pytest.mark.asyncio
async def test_polygon_split_in_later_run_is_one_repeat(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    await monitor.collect()
    monitor.state.jobs.append(
        pending_job(config, "p2", name="second", reference_time="2026-10-08T00:00:00Z")
    )
    await monitor.save()
    api.get_prediction.return_value = prediction("p2", result_id="r2")
    api.search_features.return_value = page([feature("part-a"), feature("part-b")])
    result = await monitor.collect()
    assert result["events_sent"] == 0
    assert store.value["known_events"][0]["observations"] == 2


@pytest.mark.asyncio
async def test_project_organization_parses_typed_response():
    def route(request):
        return httpx.Response(
            200, json={"records": [{"id": PROJECT, "organization_id": ORG}]}
        )

    async with client.OlmoEarthClient(
        "https://example.test", "token", transport=httpx.MockTransport(route)
    ) as http:
        assert await http.project_organization(PROJECT) == ORG


def known(external_id, change_date="2026-09-10", change_type="mining", **fields):
    return KnownEvent(
        external_id=external_id,
        geometry=AREA,
        change_type=change_type,
        change_date=change_date,
        first_seen_at=NOW,
        last_seen_at=NOW,
        first_prediction_id="p0",
        last_prediction_id="p0",
        **fields,
    )


@pytest.mark.asyncio
async def test_page_reads_only_candidates_within_date_tolerance(config, api, store):
    monitor = await seed(config, api, store)
    await store.commit(
        store.ledger, [known("near", "2026-09-01"), known("far", "2025-01-01")]
    )
    store.candidates = AsyncMock(wraps=store.candidates)
    api.search_features.return_value = page([feature(changed="2026-09-10")])
    await monitor.collect()
    (change_type, start, end), _ = store.candidates.call_args
    assert change_type == "mining"
    assert start == dt.date(2026, 8, 10) and end == dt.date(2026, 10, 11)


@pytest.mark.asyncio
async def test_undelivered_page_commits_no_event_history(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.post.side_effect = RuntimeError("lost")
    with pytest.raises(RuntimeError):
        await monitor.collect()
    assert store.events == {}
    assert len(store.ledger["outbox"]["new_events"]) == 1


@pytest.mark.asyncio
async def test_accepted_page_commits_events_with_receipts(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    await monitor.collect()
    (event,) = store.events.values()
    assert event["gundi_object_id"] == "gundi-" + event["external_id"]
    assert store.ledger["outbox"] is None


@pytest_asyncio.fixture
async def redis_state_manager():
    """A state manager on a Redis the developer opted in with OLMOEARTH_TEST_REDIS_URL.

    Never the app's own REDIS_HOST: these tests flush the database they use.
    """
    import os

    import redis.asyncio as redis

    from app.services.state import IntegrationStateManager

    url = os.environ.get("OLMOEARTH_TEST_REDIS_URL")
    if not url:
        pytest.skip("Set OLMOEARTH_TEST_REDIS_URL to a disposable Redis to run this test")
    manager = IntegrationStateManager()
    manager.db_client = redis.Redis.from_url(url)
    await manager.db_client.flushdb()
    yield manager
    await manager.db_client.flushdb()
    await manager.db_client.aclose()


async def check_store_contract(any_store):
    assert await any_store.load_ledger() is None
    await any_store.commit(
        {"identity": "x"},
        [
            known("a", "2026-09-01"),
            known("b", "2026-09-30"),
            known("c", "2026-09-10", change_type="new_road"),
        ],
    )
    assert await any_store.load_ledger() == {"identity": "x"}
    found = await any_store.candidates(
        "mining", dt.date(2026, 8, 25), dt.date(2026, 9, 15)
    )
    assert [e.external_id for e in found] == ["a"]
    assert await any_store.known_ids(["a", "c", "missing"]) == {"a", "c"}
    await any_store.commit({"identity": "y"}, [known("a", "2026-09-01", observations=3)])
    (updated,) = await any_store.candidates(
        "mining", dt.date(2026, 9, 1), dt.date(2026, 9, 1)
    )
    assert updated.observations == 3
    await any_store.save_ledger({"identity": "z"})
    assert await any_store.load_ledger() == {"identity": "z"}


@pytest.mark.asyncio
async def test_memory_store_contract():
    await check_store_contract(MemoryMonitorStore())


@pytest.mark.asyncio
async def test_redis_store_contract(redis_state_manager):
    from app.actions.monitor_store import RedisMonitorStore

    await check_store_contract(RedisMonitorStore(redis_state_manager, "integration-1"))


@pytest.mark.asyncio
async def test_redis_lock_excludes_a_second_run(redis_state_manager):
    async with redis_state_manager.lock("i", "a", ttl_seconds=30) as first:
        async with redis_state_manager.lock("i", "a", ttl_seconds=30) as second:
            assert (first, second) == (True, False)
    async with redis_state_manager.lock("i", "a", ttl_seconds=30) as again:
        assert again is True


@pytest.mark.asyncio
async def test_monitor_repeat_detection_on_redis(config, api, redis_state_manager):
    from app.actions.monitor_store import RedisMonitorStore

    store = RedisMonitorStore(redis_state_manager, "integration-1")
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    assert (await monitor.collect())["events_sent"] == 1
    monitor.state.jobs.append(
        pending_job(config, "p2", name="second", reference_time="2026-10-08T00:00:00Z")
    )
    await monitor.save()
    api.get_prediction.return_value = prediction("p2", result_id="r2")
    api.search_features.return_value = page([feature("again", changed="2026-09-12")])
    assert (await monitor.collect())["events_sent"] == 0
    (event,) = await store.candidates("mining", dt.date(2026, 9, 1), dt.date(2026, 9, 30))
    assert event.observations == 2 and event.gundi_object_id
    assert (await store.load_ledger())["outbox"] is None


@pytest.mark.asyncio
async def test_sender_setup_failure_leaves_outbox_pending(config, api, store):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.prepare.side_effect = IntegrationConnectionError("portal down")
    with pytest.raises(IntegrationConnectionError):
        await monitor.collect()
    assert store.value["outbox"]["delivery_status"] == "pending"
    monitor.sender.post.assert_not_called()
    monitor.sender.prepare.side_effect = None
    assert (await monitor.collect())["events_sent"] == 1


@pytest.mark.asyncio
async def test_event_sender_posts_once_and_reuses_its_client(mocker):
    from app.actions.delivery import GundiEventSender

    client_ = SimpleNamespace(
        post_events=AsyncMock(side_effect=httpx.ReadTimeout("lost acknowledgement"))
    )
    lookup = mocker.patch(
        "app.actions.delivery.get_gundi_sender", AsyncMock(return_value=client_)
    )
    sender = GundiEventSender("integration-1")
    await sender.prepare()
    await sender.prepare()
    with pytest.raises(httpx.ReadTimeout):
        await sender.post([{"title": "test"}])
    lookup.assert_awaited_once()
    client_.post_events.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response", [{"object_id": "1"}, ["id"], [{"errors": ["rejected"]}]]
)
async def test_event_sender_rejects_unclean_acknowledgement(mocker, response):
    from app.actions.delivery import GundiEventSender

    client_ = SimpleNamespace(post_events=AsyncMock(return_value=response))
    mocker.patch("app.actions.delivery.get_gundi_sender", AsyncMock(return_value=client_))
    sender = GundiEventSender("integration-1")
    await sender.prepare()
    with pytest.raises(IntegrationBadResponseError):
        await sender.post([{"title": "test"}])


@pytest.mark.asyncio
@pytest.mark.parametrize("receipts", [[], [None], ["id1", "id2"]])
async def test_wrong_receipt_count_or_value_pauses_outbox(config, api, store, receipts):
    monitor = await seed(config, api, store)
    api.search_features.return_value = page([feature()])
    monitor.sender.post = AsyncMock(return_value=receipts)
    with pytest.raises(IntegrationBadResponseError):
        await monitor.collect()
    assert store.value["outbox"]["delivery_status"] == "sending"


@pytest.mark.asyncio
async def test_deleted_prediction_fails_its_job_instead_of_wedging(config, api, store):
    monitor = await seed(config, api, store)
    api.get_prediction.side_effect = IntegrationBadResponseError("gone", status_code=404)
    with pytest.raises(IntegrationBadResponseError, match="no longer exists"):
        await monitor.collect()
    assert store.value["jobs"][0]["status"] == "failed"
    api.get_prediction.reset_mock()
    await make_monitor(config, api, store).collect()
    api.get_prediction.assert_not_awaited()
    assert (await make_monitor(config, api, store).submit())["submitted_prediction_id"] == "p1"


@pytest.mark.asyncio
async def test_identity_change_names_the_changed_settings(config, api, store):
    await seed(config, api, store)
    config.imagery_lag_days = 7
    with pytest.raises(IntegrationConfigurationError, match=r"\(imagery_lag_days\)"):
        await make_monitor(config, api, store).collect()
