"""Satellite evidence selection and independent attachment delivery."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from shapely.geometry import box, mapping

from app.actions.client import OlmoEarthClient
from app.actions.delivery import GundiEventSender
from app.actions.imagery import select_thumbnail
from app.actions.monitor import Monitor
from app.actions.monitor_state import MonitorState, ThumbnailDelivery
from app.services.errors import IntegrationBadResponseError

NOW = datetime(2026, 11, 19, tzinfo=timezone.utc)
EVENT = {
    "geometry": {"type": "Feature", "geometry": mapping(box(24.97, -13.02, 24.971, -13.019))},
    "event_details": {"ts_start": "2026-08-01", "ts_end": "2026-09-01"},
}


@pytest.mark.asyncio
@pytest.mark.parametrize("side,date", [("before", "2026-07-31"), ("after", "2026-09-02")])
async def test_selects_covering_scene_and_pins_render_to_scene(side, date):
    def scene(id, geom):
        return {"id": id, "properties": {"collected_at": date + "T10:00:00Z", "geometry": mapping(geom)}}
    api = SimpleNamespace(search_scenes=AsyncMock(return_value=[
        scene("partial", box(24.97, -13.02, 24.971, -13.019)),
        scene("full", box(24, -14, 26, -12)),
    ]))
    selected = await select_thumbnail(api, EVENT, side, NOW)
    assert selected["query"]["filters"] == {"id": {"inc": ["full"]}}
    assert selected["filename"] == f"{side}-{date}-sentinel2.png"
    request = api.search_scenes.call_args.args[0]
    assert request["cloud_cover"]["lte"] == 20
    dates = request["collected_at"]
    if side == "before":
        assert dates["lte"].startswith("2026-07-31")
    else:
        assert dates["gte"].startswith("2026-09-02")


@pytest.mark.asyncio
async def test_missing_dates_does_not_search():
    api = SimpleNamespace(search_scenes=AsyncMock())
    assert await select_thumbnail(api, {**EVENT, "event_details": {}}, "before", NOW) is None
    api.search_scenes.assert_not_awaited()


@pytest.mark.asyncio
async def test_render_uses_auth_header_and_rejects_non_png():
    def respond(request):
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.url.path == "/api/v1/datasets-imagery/image.png"
        assert "test-token" not in str(request.url)
        return httpx.Response(200, content=b"not png")
    async with OlmoEarthClient("https://example.test", "test-token", transport=httpx.MockTransport(respond)) as api:
        with pytest.raises(IntegrationBadResponseError, match="PNG"):
            await api.render_thumbnail({"width": 512})


@pytest.mark.asyncio
async def test_attachment_upload_uses_returned_event_id():
    sender = GundiEventSender("integration")
    sender._client = SimpleNamespace(post_event_attachments=AsyncMock(return_value=[{"object_id": "attachment"}]))
    await sender.attach("gundi-event", "before.png", b"png")
    sender._client.post_event_attachments.assert_awaited_once_with(event_id="gundi-event", attachments=[("before.png", b"png")])


@pytest.mark.asyncio
async def test_attachment_retry_does_not_resend_events(mocker):
    mocker.patch("app.actions.monitor.log_action_activity", new_callable=AsyncMock)
    monitor = Monitor("integration", None, SimpleNamespace(render_thumbnail=AsyncMock(return_value=b"png")),
                      SimpleNamespace(set_state=AsyncMock()),
                      SimpleNamespace(send=AsyncMock(), attach=AsyncMock(side_effect=RuntimeError("offline"))), NOW)
    item = ThumbnailDelivery(event_id="event", event=EVENT, side="before", selection={"filename": "before.png", "query": {"width": 512}})
    monitor.state = MonitorState(identity={}, thumbnails=[item])
    await monitor.deliver_thumbnails()
    assert monitor.state.thumbnails[0].attempts == 1
    monitor.sender.send.assert_not_awaited()
    monitor.sender.attach.side_effect = None
    await monitor.deliver_thumbnails()
    assert monitor.state.thumbnails == []
    assert monitor.sender.attach.await_count == 2


@pytest.mark.asyncio
async def test_attachment_failure_stops_after_three_attempts(mocker):
    mocker.patch("app.actions.monitor.log_action_activity", new_callable=AsyncMock)
    monitor = Monitor("integration", None, SimpleNamespace(render_thumbnail=AsyncMock(side_effect=RuntimeError())),
                      SimpleNamespace(set_state=AsyncMock()), SimpleNamespace(attach=AsyncMock()), NOW)
    monitor.state = MonitorState(identity={}, thumbnails=[ThumbnailDelivery(
        event_id="event", event=EVENT, side="after", attempts=2,
        selection={"filename": "after.png", "query": {}},
    )])
    await monitor.deliver_thumbnails()
    assert not monitor.state.thumbnails
    monitor.sender.attach.assert_not_awaited()
