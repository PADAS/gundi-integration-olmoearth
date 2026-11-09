from functools import wraps
from pydantic import BaseModel, SecretBytes, SecretStr
from pydantic.fields import Field
from pydantic.class_validators import validator
from typing import Any, Dict, Optional, Union, List, Annotated
from gundi_core.commands import RunIntegrationAction
from app import settings
from .activity_logger import publish_event, publish_events
from .gundi import _block_if_ephemeral


def _unwrap_secrets(value):
    if isinstance(value, (SecretStr, SecretBytes)):
        secret = value.get_secret_value()
        return secret.decode("utf-8") if isinstance(secret, bytes) else secret
    if isinstance(value, dict):
        return {key: _unwrap_secrets(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_unwrap_secrets(item) for item in value]
    return value


def _config_overrides(config) -> Optional[dict]:
    """The config as the triggered action receives it, secrets unwrapped.

    The command is serialized with json.dumps(default=str), and str() of a
    SecretStr is its mask, so a per-source config carrying a credential would
    otherwise reach the sub-action as the literal '**********'.
    """
    return _unwrap_secrets(config.dict()) if config else None


async def trigger_action(integration_id: str, action_id: str, config=None):
    """
    Publishes a command message in the actions topic to trigger an action.
    Use this function to trigger other actions from the integration.
    :param integration_id: uuid of the integration
    :param action_id: slug id of the action
    :param config: configuration model
    :return:
    """
    # A reference/auth handler on the ephemeral path has no saved integration
    # to trigger against: async mode would drop the command silently and sync
    # mode would run a portal lookup that fails. Raise instead.
    _block_if_ephemeral("trigger_action")
    config_overrides = _config_overrides(config)
    run_action_command = RunIntegrationAction(
        integration_id=integration_id,
        action_id=action_id,
        config_overrides=config_overrides
    )
    if settings.TRIGGER_ACTIONS_ALWAYS_SYNC:  # For testing or local development
        from .action_runner import execute_action
        return await execute_action(
            integration_id=integration_id,
            action_id=action_id,
            config_overrides=config_overrides
        )
    else:
        if not settings.INTEGRATION_COMMANDS_TOPIC:
            error_msg = "Please set INTEGRATION_COMMANDS_TOPIC in the environment to trigger actions from the integration."
            raise ValueError(error_msg)
        return await publish_event(run_action_command, settings.INTEGRATION_COMMANDS_TOPIC)



async def trigger_actions(integration_id: str, action_id: str, configs):
    """
    Triggers the same action once per config, publishing all the command
    messages to the actions topic in one batch.
    Use this instead of calling trigger_action in a loop when fanning out over
    many sources: one publish request (per 1,000 commands) instead of one
    session, token and round trip per command.
    :param integration_id: uuid of the integration
    :param action_id: slug id of the action
    :param configs: one configuration model per triggered action
    :return:
    """
    _block_if_ephemeral("trigger_actions")
    commands = [
        RunIntegrationAction(
            integration_id=integration_id,
            action_id=action_id,
            config_overrides=_config_overrides(config)
        )
        for config in configs
    ]
    if not commands:
        # Nothing to fan out: each mode's empty result, without requiring the
        # topic or opening a session.
        return [] if settings.TRIGGER_ACTIONS_ALWAYS_SYNC else {"messageIds": []}
    if settings.TRIGGER_ACTIONS_ALWAYS_SYNC:  # For testing or local development
        from .action_runner import execute_action
        return [
            await execute_action(
                integration_id=integration_id,
                action_id=action_id,
                config_overrides=command.config_overrides
            )
            for command in commands
        ]
    if not settings.INTEGRATION_COMMANDS_TOPIC:
        error_msg = "Please set INTEGRATION_COMMANDS_TOPIC in the environment to trigger actions from the integration."
        raise ValueError(error_msg)
    return await publish_events(commands, settings.INTEGRATION_COMMANDS_TOPIC)

class CrontabSchedule(BaseModel):
    minute: str = Field(
        "*",
        regex=r"^(\*|([0-5]?\d)(,([0-5]?\d))*|([0-5]?\d-[0-5]?\d)(/\d+)?|\*(/\d+)?)$"
    )
    hour: str = Field(
        "*",
        regex=r"^(\*|([01]?\d|2[0-3])(,([01]?\d|2[0-3]))*|([01]?\d|2[0-3]-[01]?\d|2[0-3])(/\d+)?|\*(/\d+)?)$"
    )
    day_of_week: str = Field(
        "*",
        regex=r"^(\*|[0-6](,[0-6])*|([0-6]-[0-6])(/\d+)?|\*(/\d+)?)$"
    )
    day_of_month: str = Field(
        "*",
        regex=r"^(\*|([1-9]|[12]\d|3[01])(,([1-9]|[12]\d|3[01]))*|([1-9]|[12]\d|3[01]-[1-9]|[12]\d|3[01])(/\d+)?|\*(/\d+)?)$"
    )
    month_of_year: str = Field(
        "*",
        regex=r"^(\*|([1-9]|1[0-2])(,([1-9]|1[0-2]))*|([1-9]|1[0-2]-[1-9]|1[0-2])(/\d+)?|\*(/\d+)?)$"
    )
    tz_offset: int = Field(
        0,
        description="Timezone offset from UTC, e.g., 0 for UTC, -5 for UTC-5, +2 for UTC+2"
    )

    @validator("tz_offset")
    def validate_timezone(cls, value):
        """Validate that timezone is an integer between -12 and +14."""
        if not (-12 <= value <= 14):
            raise ValueError("Timezone offset must be between -12 and +14.")
        return value

    @validator("minute", "hour", "day_of_week", "day_of_month", "month_of_year")
    def validate_crontab_field(cls, value, field):
        if not value:
            raise ValueError(f"{field.name} cannot be empty.")
        return value

    # build from crontab string
    @classmethod
    def parse_obj_from_crontab(cls, crontab: str):
        parts = crontab.split()
        if len(parts) == 6:
            minute, hour, day_of_month, month_of_year, day_of_week, tz_offset = parts
        elif len(parts) == 5:
            minute, hour, day_of_month, month_of_year, day_of_week = parts
            tz_offset = 0
        else:
            raise ValueError("Invalid crontab format. Must have 5 or 6 fields.")

        return cls(
            minute=minute,
            hour=hour,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
            day_of_week=day_of_week,
            tz_offset=int(tz_offset)
        )


# Defines when a periodic action runs. Can receive a CrontabSchedule object or a string as an argument
def crontab_schedule(crontab: Union[CrontabSchedule, str]):
    def decorator(func):
        if isinstance(crontab, str):
            schedule = CrontabSchedule.parse_obj_from_crontab(crontab)
        else:
            schedule = crontab
        setattr(func, "crontab_schedule", schedule)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        return wrapper
    return decorator

