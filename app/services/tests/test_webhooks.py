"""The webhook processor's own failure events, published outside the
webhook_activity_logger decorator (which redacts for itself)."""
import pydantic
import pytest
from gundi_core.schemas.v2 import Integration

from app import settings
from app.conftest import AsyncMock
from app.services.redaction import REDACTED
from app.services.webhooks import process_webhook
from app.webhooks.core import WebhookConfiguration, WebhookPayload


class _ConfigWithSecrets(WebhookConfiguration):
    region: str
    api_key: pydantic.SecretStr
    login_code: pydantic.SecretStr  # secret only by declaration, not by name


class _StrictPayload(WebhookPayload):
    device_id: str
    lat: float


class _Request:
    def __init__(self, body):
        self.headers = {"x-consumer-username": "integration:abced116-efb4-4fb1-9d68-0ecc4b0996b2"}
        self.query_params = {}
        self._body = body

    async def json(self):
        return self._body


_WEBHOOK_CONFIG = {"region": "eu", "api_key": "k-123", "login_code": "c-456"}


@pytest.fixture
def integration_with_secret_webhook_config(integration_v2_with_webhook):
    data = integration_v2_with_webhook.dict()
    data["webhook_configuration"]["data"] = dict(_WEBHOOK_CONFIG)
    return Integration.parse_obj(data)


@pytest.fixture
def webhook_mocks(mocker, mock_publish_event, integration_with_secret_webhook_config):
    mocker.patch(
        "app.services.webhooks.config_manager.get_integration_details",
        AsyncMock(return_value=integration_with_secret_webhook_config),
    )
    mocker.patch("app.services.webhooks.publish_event", mock_publish_event)
    mocker.patch.object(settings, "PROCESS_WEBHOOKS_IN_BACKGROUND", False)
    return mock_publish_event


def _published_failure(mock_publish_event):
    assert mock_publish_event.call_count == 1
    event = mock_publish_event.call_args.kwargs["event"]
    assert type(event).__name__ == "IntegrationWebhookFailed"
    return event.payload


@pytest.mark.asyncio
async def test_payload_parse_failure_event_redacts_the_webhook_config(mocker, webhook_mocks):
    handler = AsyncMock()
    mocker.patch("app.services.webhooks.get_webhook_handler", return_value=(handler, _StrictPayload, _ConfigWithSecrets))

    await process_webhook(_Request({"device_id": "d1"}))  # lat missing

    payload = _published_failure(webhook_mocks)
    assert "Error parsing payload" in payload.error
    assert payload.config_data == {"region": "eu", "api_key": REDACTED, "login_code": REDACTED}
    assert not handler.called


@pytest.mark.asyncio
async def test_handler_failure_event_redacts_the_webhook_config(mocker, webhook_mocks):
    handler = AsyncMock(side_effect=RuntimeError("provider exploded"))
    mocker.patch("app.services.webhooks.get_webhook_handler", return_value=(handler, _StrictPayload, _ConfigWithSecrets))

    await process_webhook(_Request({"device_id": "d1", "lat": 1.0}))

    payload = _published_failure(webhook_mocks)
    assert "provider exploded" in payload.error
    assert payload.config_data == {"region": "eu", "api_key": REDACTED, "login_code": REDACTED}
    assert handler.call_args.kwargs["webhook_config"].api_key.get_secret_value() == "k-123"


@pytest.mark.asyncio
async def test_failure_before_the_config_is_read_still_publishes_an_event(mocker, webhook_mocks):
    # A body that is not JSON fails before the webhook configuration has been
    # read; the failure event is published with an empty one, not lost to a
    # NameError inside the except block.
    class _BadRequest(_Request):
        async def json(self):
            raise ValueError("not JSON")

    handler = AsyncMock()
    mocker.patch("app.services.webhooks.get_webhook_handler", return_value=(handler, _StrictPayload, _ConfigWithSecrets))

    await process_webhook(_BadRequest(None))

    payload = _published_failure(webhook_mocks)
    assert "not JSON" in payload.error
    assert payload.config_data == {}
