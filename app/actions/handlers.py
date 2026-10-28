"""Gundi actions for scheduling and collecting an OlmoEarth LCC monitor."""

from datetime import datetime, timezone
from typing import Awaitable, Callable

from app import settings
from app.actions.core import action_title
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger
from app.services.errors import IntegrationConfigurationError
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action

from . import client
from .configurations import (
    AuthenticateConfig,
    PullEventsConfig,
    SubmitPredictionsConfig,
)
from .delivery import GundiEventSender
from .monitor import Monitor
from .monitor_state import STATE_ACTION

state_manager = IntegrationStateManager()
# The runner cancels every action after MAX_ACTION_EXECUTION_TIME, so a run always
# releases the lock before it expires; the expiry only frees a lock a crash left behind.
RUN_LOCK_TTL_SECONDS = settings.MAX_ACTION_EXECUTION_TIME + 300


def get_auth_config(integration) -> AuthenticateConfig:
    config = find_config_for_action(integration.configurations, "auth")
    if not config:
        raise IntegrationConfigurationError(
            "Configure the OlmoEarth authentication action first."
        )
    return AuthenticateConfig.parse_obj(config.data)


def get_monitor_config(integration) -> PullEventsConfig:
    saved = find_config_for_action(integration.configurations, "pull_events")
    if not saved:
        raise IntegrationConfigurationError(
            "Save the Collect LCC Events monitor configuration first."
        )
    return PullEventsConfig.parse_obj(saved.data)


def client_for(integration, auth_config=None) -> client.OlmoEarthClient:
    auth_config = auth_config or get_auth_config(integration)
    return client.OlmoEarthClient(
        base_url=integration.base_url,
        api_token=auth_config.api_token.get_secret_value(),
    )


@action_title("Authenticate")
async def action_auth(integration, action_config: AuthenticateConfig) -> dict:
    async with client_for(integration, action_config) as api:
        total = await api.verify_token()
    return {"valid_credentials": True, "predictions_visible": total}


async def run_monitor(
    integration,
    config: PullEventsConfig,
    work: Callable[[Monitor], Awaitable[dict]],
) -> dict:
    integration_id = str(integration.id)
    # Every action rewrites the one ledger value, so runs must not overlap (a manual
    # run, or a PubSub redelivery, can start while another is in progress).
    if not await state_manager.set_if_absent(
        integration_id, STATE_ACTION, source_id="lock", ttl_seconds=RUN_LOCK_TTL_SECONDS
    ):
        return {"status": "already_running"}
    try:
        async with client_for(integration) as api:
            monitor = Monitor(
                integration_id,
                config,
                api,
                state_manager,
                GundiEventSender(integration_id),
                datetime.now(timezone.utc),
            )
            return await work(monitor)
    finally:
        await state_manager.delete_state(integration_id, STATE_ACTION, source_id="lock")


@action_title("Submit LCC Predictions")
@crontab_schedule("0 */4 * * *")
@activity_logger()
async def action_submit_predictions(
    integration, action_config: SubmitPredictionsConfig
) -> dict:
    return await run_monitor(integration, get_monitor_config(integration), Monitor.submit)


@action_title("Collect LCC Events")
@crontab_schedule("15 */4 * * *")
@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig) -> dict:
    return await run_monitor(integration, action_config, Monitor.collect)

