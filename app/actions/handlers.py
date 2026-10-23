"""Gundi actions for scheduling and collecting an OlmoEarth LCC monitor."""

import asyncio
from datetime import datetime, timezone

from app.actions.core import ReferenceDataResponse, ReferenceOption, action_title
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, ephemeral_run
from app.services.errors import IntegrationConfigurationError
from app.services.gundi import send_events_once_to_gundi
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
from .monitor import Monitor
from .monitor_state import STATE_ACTION

state_manager = IntegrationStateManager()


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
    integration, config: PullEventsConfig, operation: str, resolution=None
) -> dict:
    if ephemeral_run.get():
        raise IntegrationConfigurationError(
            "Save this integration before running the monitor."
        )
    integration_id = str(integration.id)
    lock = state_manager.db_client.lock(
        f"{STATE_ACTION}:lock:{integration_id}", timeout=900, blocking=False
    )
    if not await lock.acquire():
        return {"status": "already_running"}
    try:

        async def send(events):
            return await send_events_once_to_gundi(
                events=events, integration_id=integration_id
            )

        async with client_for(integration) as api:
            monitor = Monitor(
                integration_id,
                config,
                api,
                state_manager,
                send,
                datetime.now(timezone.utc),
            )
            if operation == "resolve":
                work = monitor.resolve_delivery(resolution)
            else:
                work = monitor.submit() if operation == "submit" else monitor.collect()
            # Finish/cancel before the shared lock expires, even on a stalled provider.
            return await asyncio.wait_for(work, timeout=600)
    finally:
        await lock.release()


@action_title("Submit LCC Predictions")
@crontab_schedule("0 */4 * * *")
@activity_logger()
async def action_submit_predictions(
    integration, action_config: SubmitPredictionsConfig
) -> dict:
    return await run_monitor(integration, get_monitor_config(integration), "submit")


@action_title("Collect LCC Events")
@crontab_schedule("15 */4 * * *")
@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig) -> dict:
    return await run_monitor(integration, action_config, "collect")


@action_title("Resolve LCC Delivery")
@activity_logger()
async def action_resolve_delivery(
    integration, action_config: ResolveDeliveryConfig
) -> dict:
    return await run_monitor(
        integration, get_monitor_config(integration), "resolve", action_config
    )
