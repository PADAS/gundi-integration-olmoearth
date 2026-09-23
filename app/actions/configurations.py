"""Configuration models for the OlmoEarth integration.

Three actions, three configs:

  auth              the API token, validated against the predictions search
  pull_events       the scheduled ingest: one feature search -> Gundi events
  list_predictions  a reference lookup the portal renders as a dropdown

The site URL is not a field here — it comes from the integration's own
`base_url`, which is where the portal already asks for it.
"""
from typing import Any, Dict, Optional

import pydantic

from app.services.utils import (
    FieldWithUIOptions,
    GlobalUISchemaOptions,
    OptionalStringType,
    UIOptions,
)

from .core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    PullActionConfiguration,
    ReferenceActionConfiguration,
)


# The `prediction_results` filters that narrow the ingest. A bare class
# attribute on PullEventsConfig would become a pydantic field, so it lives here.
SCOPE_FIELDS = ("model_id", "project_id", "organization_id", "target_area_id")


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_token: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="API Token",
        description=(
            "Bearer token for the OlmoEarth API. Sent as "
            "`Authorization: Bearer <token>` on every request."
        ),
        format="password",
        ui_options=UIOptions(widget="password"),
    )
    ui_global_options = GlobalUISchemaOptions(order=["api_token"])


class PullEventsConfig(PullActionConfiguration):
    """The ingest query.

    The first five fields are the *scope*: which Prediction Results this
    integration reads. They go into the `prediction_results` half of the
    feature search, which is the half OlmoEarth access-controls. At least one
    of them is required — an unscoped search returns every Result the token can
    see, which is a feed nobody asked for and, past a thousand Results, a 400.
    """

    model_id: OptionalStringType = FieldWithUIOptions(
        None,
        title="Model ID",
        description="Ingest detections produced by this OlmoEarth model.",
    )
    project_id: OptionalStringType = FieldWithUIOptions(
        None,
        title="Project ID",
        description="Ingest detections from Predictions in this project.",
    )
    organization_id: OptionalStringType = FieldWithUIOptions(
        None,
        title="Organization ID",
        description="Ingest detections from Prediction Results owned by this organization.",
    )
    target_area_id: OptionalStringType = FieldWithUIOptions(
        None,
        title="Target Area ID",
        description=(
            "Ingest detections from Predictions that targeted this registered "
            "OlmoEarth Area. This is the scope to use for a monitored area: it "
            "prunes on the provider before any geometry is read."
        ),
    )
    area_of_interest: Optional[Dict[str, Any]] = FieldWithUIOptions(
        None,
        title="Area Of Interest",
        description=(
            "Optional GeoJSON geometry. Only detections intersecting it are "
            'ingested, e.g. {"type": "Polygon", "coordinates": [[...]]}. '
            "Applied to the detections themselves, so unlike Target Area ID it "
            "also works for Predictions created from an uploaded GeoJSON."
        ),
        ui_options=UIOptions(widget="textarea"),
    )
    lookback_days: int = FieldWithUIOptions(
        7,
        ge=1,
        le=365,
        title="Lookback Days",
        description=(
            "How far back the first run reaches. Later runs resume from where "
            "the last one finished and ignore this."
        ),
        ui_options=UIOptions(widget="range"),
    )
    min_confidence: Optional[float] = FieldWithUIOptions(
        None,
        ge=0.0,
        le=1.0,
        title="Minimum Confidence",
        description=(
            "Optional. Drop features whose confidence property is below this "
            "value. The filter runs on the provider, not here."
        ),
    )
    confidence_property: str = FieldWithUIOptions(
        "confidence",
        title="Confidence Property",
        description=(
            "Name of the feature property the minimum-confidence filter reads. "
            "Models differ — some call it `score`."
        ),
    )
    event_type: str = FieldWithUIOptions(
        "olmoearth_detection",
        title="Gundi Event Type",
        description="Event type recorded in Gundi for every ingested feature.",
    )
    event_title_prefix: str = FieldWithUIOptions(
        "OlmoEarth",
        title="Event Title Prefix",
        description=(
            "Leads the title of every event, e.g. `OlmoEarth detection 1234`. "
            "A cross-Result search does not carry a Prediction's name, so this "
            "is what names the feed."
        ),
    )
    include_geometry: bool = FieldWithUIOptions(
        True,
        title="Include Full Geometry",
        description=(
            "Send the feature's full GeoJSON geometry alongside the point "
            "location, so a polygon detection stays a polygon downstream."
        ),
        ui_options=UIOptions(widget="radio"),
    )
    max_features_per_run: int = FieldWithUIOptions(
        10_000,
        ge=1,
        title="Max Features Per Run",
        description=(
            "Stop after this many detections in one run, so an oversized "
            "backlog cannot stall the schedule. The watermark still advances "
            "over what was read, so the next run picks up where this one "
            "stopped."
        ),
    )
    feature_page_size: int = FieldWithUIOptions(
        500,
        ge=1,
        le=1000,
        title="Feature Page Size",
        description="How many detections to request per page.",
    )
    events_per_request: int = FieldWithUIOptions(
        200,
        ge=1,
        le=1000,
        title="Events Per Request",
        description="How many events to send to Gundi in one request.",
    )

    @pydantic.root_validator
    def at_least_one_scope(cls, values):
        """Refuse a query with nothing to narrow it.

        Left unscoped the search returns every Prediction Result the token can
        read. That is either a silent firehose or, past the provider's
        thousand-Result ceiling, a 400 on every run — and both are worse found
        here than at 4am on a schedule.
        """
        if not any(values.get(field) for field in SCOPE_FIELDS):
            raise ValueError(
                "Set at least one of Model ID, Project ID, Organization ID or "
                "Target Area ID, so the ingest reads a defined set of "
                "Prediction Results rather than everything the token can see."
            )
        return values

    ui_global_options = GlobalUISchemaOptions(
        order=[
            "model_id",
            "project_id",
            "organization_id",
            "target_area_id",
            "area_of_interest",
            "lookback_days",
            "min_confidence",
            "confidence_property",
            "event_type",
            "event_title_prefix",
            "include_geometry",
            "max_features_per_run",
            "feature_page_size",
            "events_per_request",
            "run_on_schedule",
        ],
    )


class ListPredictionsConfig(ReferenceActionConfiguration):
    """Query for the reference lookup: the config model *is* the query.

    Stateless — the portal sends these as overrides when a user opens the
    dropdown, and nothing is stored.
    """

    model_id: OptionalStringType = pydantic.Field(
        None,
        title="Model ID",
        description="Optional. List only predictions from this model.",
    )
    project_id: OptionalStringType = pydantic.Field(
        None,
        title="Project ID",
        description="Optional. List only predictions from this project.",
    )
    target_area_id: OptionalStringType = pydantic.Field(
        None,
        title="Target Area ID",
        description="Optional. List only predictions that targeted this Area.",
    )
    status: OptionalStringType = pydantic.Field(
        "completed",
        title="Status",
        description="Optional. List only predictions in this status.",
    )
    limit: int = pydantic.Field(
        100,
        ge=1,
        le=500,
        title="Limit",
        description="How many predictions to return, newest first.",
    )
