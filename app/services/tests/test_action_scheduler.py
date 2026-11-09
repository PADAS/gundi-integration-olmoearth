import json

import pydantic
import pytest

from app import settings
from app.conftest import AsyncMock
from app.services.action_scheduler import trigger_action, trigger_actions


class _Config:
    def __init__(self, **values):
        self._values = values

    def dict(self):
        return dict(self._values)


@pytest.mark.asyncio
async def test_trigger_actions_publishes_every_command_in_one_batch(mocker):
    mock_publish_events = AsyncMock(return_value={"messageIds": ["1", "2", "3"]})
    mocker.patch("app.services.action_scheduler.publish_events", mock_publish_events)
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", False)
    mocker.patch.object(settings, "INTEGRATION_COMMANDS_TOPIC", "actions-topic")
    configs = [_Config(collar_id=f"ST-{i}") for i in range(3)]

    await trigger_actions(integration_id="int-1", action_id="read_per_collar", configs=configs)

    mock_publish_events.assert_called_once()
    events, topic_name = mock_publish_events.call_args.args
    assert topic_name == "actions-topic"
    assert [e.config_overrides for e in events] == [{"collar_id": "ST-0"}, {"collar_id": "ST-1"}, {"collar_id": "ST-2"}]
    assert all(e.integration_id == "int-1" and e.action_id == "read_per_collar" for e in events)


@pytest.mark.asyncio
async def test_trigger_actions_with_no_configs_publishes_nothing(mocker):
    mock_publish_events = AsyncMock()
    mocker.patch("app.services.action_scheduler.publish_events", mock_publish_events)
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", False)
    mocker.patch.object(settings, "INTEGRATION_COMMANDS_TOPIC", "actions-topic")

    response = await trigger_actions(integration_id="int-1", action_id="read_per_collar", configs=[])

    assert not mock_publish_events.called
    # The same shape publish_events gives an empty list.
    assert response == {"messageIds": []}


@pytest.mark.asyncio
async def test_trigger_actions_runs_each_action_inline_in_sync_mode(mocker):
    mock_execute_action = AsyncMock(return_value={"ok": True})
    mocker.patch("app.services.action_runner.execute_action", mock_execute_action)
    mock_publish_events = AsyncMock()
    mocker.patch("app.services.action_scheduler.publish_events", mock_publish_events)
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", True)
    configs = [_Config(collar_id="ST-0"), _Config(collar_id="ST-1")]

    await trigger_actions(integration_id="int-1", action_id="read_per_collar", configs=configs)

    assert not mock_publish_events.called
    assert [c.kwargs["config_overrides"] for c in mock_execute_action.call_args_list] == [
        {"collar_id": "ST-0"}, {"collar_id": "ST-1"}
    ]


@pytest.mark.asyncio
async def test_trigger_actions_requires_the_commands_topic(mocker):
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", False)
    mocker.patch.object(settings, "INTEGRATION_COMMANDS_TOPIC", "")

    with pytest.raises(ValueError, match="INTEGRATION_COMMANDS_TOPIC"):
        await trigger_actions(integration_id="int-1", action_id="read_per_collar", configs=[_Config()])


@pytest.mark.asyncio
async def test_trigger_actions_is_blocked_on_ephemeral_run(mocker):
    from app.services.activity_logger import ephemeral_run
    from app.services.gundi import EphemeralWriteBlocked
    mock_publish_events = AsyncMock()
    mocker.patch("app.services.action_scheduler.publish_events", mock_publish_events)

    token = ephemeral_run.set(True)
    try:
        with pytest.raises(EphemeralWriteBlocked):
            await trigger_actions(integration_id="synthetic-uuid", action_id="read_per_collar", configs=[_Config()])
    finally:
        ephemeral_run.reset(token)

    assert not mock_publish_events.called


@pytest.mark.asyncio
async def test_trigger_actions_with_no_configs_needs_no_topic_and_returns_the_sync_shape(mocker):
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", False)
    mocker.patch.object(settings, "INTEGRATION_COMMANDS_TOPIC", "")
    assert await trigger_actions(integration_id="int-1", action_id="read_per_collar", configs=[]) == {"messageIds": []}

    mock_execute_action = AsyncMock()
    mocker.patch("app.services.action_runner.execute_action", mock_execute_action)
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", True)
    assert await trigger_actions(integration_id="int-1", action_id="read_per_collar", configs=[]) == []
    assert not mock_execute_action.called


class _PerCollarConfig(pydantic.BaseModel):
    collar_id: str
    api_key: pydantic.SecretStr
    credentials: dict


@pytest.mark.asyncio
async def test_trigger_actions_passes_secrets_to_the_triggered_action_unmasked(mocker):
    # The command is serialized with json.dumps(default=str), and str() of a
    # SecretStr is its mask: the sub-action would authenticate with
    # '**********' (the mask is what config.dict() keeps).
    mock_publish_events = AsyncMock(return_value={"messageIds": ["1"]})
    mocker.patch("app.services.action_scheduler.publish_events", mock_publish_events)
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", False)
    mocker.patch.object(settings, "INTEGRATION_COMMANDS_TOPIC", "actions-topic")
    config = _PerCollarConfig(
        collar_id="ST-0", api_key="plain-key", credentials={"token": pydantic.SecretStr("nested-token")},
    )

    await trigger_actions(integration_id="int-1", action_id="read_per_collar", configs=[config])

    (command,), _ = mock_publish_events.call_args.args
    assert command.config_overrides == {
        "collar_id": "ST-0", "api_key": "plain-key", "credentials": {"token": "nested-token"},
    }
    assert json.loads(json.dumps(command.dict(), default=str))["config_overrides"]["api_key"] == "plain-key"


@pytest.mark.asyncio
async def test_trigger_action_passes_secrets_to_the_triggered_action_unmasked(mocker):
    mock_publish_event = AsyncMock(return_value={"messageIds": ["1"]})
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch.object(settings, "TRIGGER_ACTIONS_ALWAYS_SYNC", False)
    mocker.patch.object(settings, "INTEGRATION_COMMANDS_TOPIC", "actions-topic")
    config = _PerCollarConfig(collar_id="ST-0", api_key="plain-key", credentials={})

    await trigger_action(integration_id="int-1", action_id="read_per_collar", config=config)

    command, _ = mock_publish_event.call_args.args
    assert command.config_overrides == {"collar_id": "ST-0", "api_key": "plain-key", "credentials": {}}
