"""Gundi actions for scheduling and collecting an OlmoEarth LCC monitor."""

from datetime import datetime, timezone
from typing import Awaitable, Callable

from app import settings
from app.actions.core import ReferenceDataResponse, ReferenceOption, action_title
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger
from app.services.errors import IntegrationConfigurationError
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action

from . import client
from .configurations import (
    AuthenticateConfig,
    ListPredictionsConfig,
    PullEventsConfig,
    ResolveDeliveryConfig,
    SubmitPredictionsConfig,
)
from .delivery import GundiEventSender
from .monitor import Monitor
from .monitor_state import STATE_ACTION
from .monitor_store import RedisMonitorStore

state_manager = IntegrationStateManager()
# The runner cancels every action after MAX_ACTION_EXECUTION_TIME; the margin lets a
# cancelled run unwind and release the lock before it could expire under it.
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


@action_title("List Predictions")
async def action_list_predictions(
    integration, action_config: ListPredictionsConfig
) -> dict:
    query = client.PredictionSearchRequest(
        project_id=client.KeywordFilter(eq=str(action_config.project_id)),
        limit=action_config.limit,
    )
    if action_config.model_id:
        query.model_id = client.KeywordFilter(eq=str(action_config.model_id))
    async with client_for(integration) as api:
        response = await api.search_predictions(query)
    return ReferenceDataResponse(
        options=[
            ReferenceOption(
                value=p.id, label=p.name or p.id, description=p.status, group=p.model_id
            )
            for p in response.records
        ],
        truncated=response.meta.total is not None
        and response.meta.total > len(response.records),
    ).dict()


async def run_monitor(
    integration,
    config: PullEventsConfig,
    work: Callable[[Monitor], Awaitable[dict]],
) -> dict:
    integration_id = str(integration.id)
    async with state_manager.lock(
        integration_id, STATE_ACTION, ttl_seconds=RUN_LOCK_TTL_SECONDS
    ) as acquired:
        if not acquired:
            return {"status": "already_running"}
        async with client_for(integration) as api:
            monitor = Monitor(
                integration_id,
                config,
                api,
                RedisMonitorStore(state_manager, integration_id),
                GundiEventSender(integration_id),
                datetime.now(timezone.utc),
            )
            return await work(monitor)


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


@action_title("Resolve LCC Delivery")
@activity_logger()
async def action_resolve_delivery(
    integration, action_config: ResolveDeliveryConfig
) -> dict:
    return await run_monitor(
        integration,
        get_monitor_config(integration),
        lambda monitor: monitor.resolve_delivery(action_config),
    )
