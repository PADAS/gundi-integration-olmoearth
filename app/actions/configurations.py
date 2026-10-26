"""One LCC monitor per saved Gundi integration."""

from datetime import date
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

import pydantic
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from app.services.utils import FieldWithUIOptions, UIOptions

from .core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    GenericActionConfiguration,
    PullActionConfiguration,
    ReferenceActionConfiguration,
)


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_token: pydantic.SecretStr = FieldWithUIOptions(
        ..., title="OlmoEarth API token", ui_options=UIOptions(widget="password")
    )


class Cadence(str, Enum):
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class PostChange(str, Enum):
    VEGETATION_GROWTH = "vegetation_growth"
    NEW_BUILDING = "new_building"
    NEW_ROAD = "new_road"
    NEW_INFRASTRUCTURE = "new_infrastructure"
    NEW_CROP_FIELD = "new_crop_field"
    NEW_AQUAFARM = "new_aquafarm"
    SITE_CLEARING = "site_clearing"
    WATER_EXPAND = "water_expand"
    MINING = "mining"
    NEW_CROP_STRUCTURE = "new_crop_structure"
    SELECTIVE_LOGGING = "selective_logging"
    LANDSLIDE = "landslide"
    SETTLEMENT = "settlement"


def polygon_geometry(value: dict) -> dict:
    polygon_shape(value)
    return value


def polygon_shape(value: dict) -> BaseGeometry:
    """The shapely geometry for a regional, valid WGS84 (Multi)Polygon."""
    try:
        geom = shape(value)
        if (
            geom.geom_type not in ("Polygon", "MultiPolygon")
            or geom.is_empty
            or not geom.is_valid
        ):
            raise ValueError("Expected a nonempty valid Polygon or MultiPolygon")
        xmin, ymin, xmax, ymax = geom.bounds
        if (
            not (-180 <= xmin <= xmax <= 180 and -90 <= ymin <= ymax <= 90)
            or xmax - xmin >= 180
        ):
            raise ValueError(
                "Use a regional WGS84 polygon that does not cross the antimeridian"
            )
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("Expected a GeoJSON Polygon or MultiPolygon geometry") from exc
    return geom


class PullEventsConfig(PullActionConfiguration):
    project_id: UUID = FieldWithUIOptions(..., title="OlmoEarth project")
    organization_id: Optional[UUID] = FieldWithUIOptions(
        None,
        title="OlmoEarth organization",
        description="If supplied, must own the selected project.",
    )
    model_id: UUID = FieldWithUIOptions(
        ...,
        title="LCC model",
        description="Use a model configured to vectorize post_change and emit ISO date properties.",
    )
    area_of_interest: Dict[str, Any] = FieldWithUIOptions(
        ..., title="Area (GeoJSON geometry)", ui_options=UIOptions(widget="textarea")
    )
    cadence: Cadence = FieldWithUIOptions(
        Cadence.MONTHLY,
        title="Prediction frequency",
        ui_options=UIOptions(widget="select"),
    )
    schedule_start: date = FieldWithUIOptions(
        ...,
        title="First prediction date (UTC)",
        description="Missed schedule slots are processed oldest first.",
    )
    imagery_lag_days: int = pydantic.Field(
        14,
        ge=0,
        le=365,
        description="Reference date precedes each schedule slot by this many days; verify for the chosen model.",
    )
    select_all_types: bool = FieldWithUIOptions(True, title="Select all change types")
    post_change_types: List[PostChange] = FieldWithUIOptions(
        default_factory=list,
        title="Change types",
        unique_items=True,
        ui_options=UIOptions(widget="select"),
    )
    change_date_property: str = pydantic.Field(
        ...,
        min_length=1,
        description="Verified polygon property containing an ISO date, not a timestep index or encoded month count.",
    )
    change_date_precision: str = pydantic.Field("month", regex="^(day|month)$")
    change_date_aggregation: str = pydantic.Field(
        ...,
        min_length=1,
        description="Document the model's reduction, e.g. percentile_10. This does not change the model config.",
    )
    changes_since: Optional[date] = pydantic.Field(
        None,
        description="Optional fixed event-date cutoff. Never advanced as an ingestion watermark.",
    )
    event_type: str = pydantic.Field("olmoearth_detection", min_length=1)
    feature_page_size: int = pydantic.Field(100, ge=1, le=1000)
    pages_per_poll: int = pydantic.Field(5, ge=1, le=20)
    overlap_threshold: float = pydantic.Field(
        0.7,
        gt=0,
        le=1,
        description="Minimum polygon intersection-over-union for a repeat detection across runs.",
    )
    date_tolerance_days: int = pydantic.Field(31, ge=0, le=365)

    _valid_area = pydantic.validator("area_of_interest", allow_reuse=True)(
        polygon_geometry
    )

    @pydantic.root_validator
    def require_selected_types(cls, values):
        if not values.get("select_all_types") and not values.get("post_change_types"):
            raise ValueError("Select at least one change type or enable Select all")
        return values

    @property
    def selected_types(self) -> List[str]:
        types = list(PostChange) if self.select_all_types else self.post_change_types
        return [value.value for value in types]


class SubmitPredictionsConfig(PullActionConfiguration):
    run_on_schedule: bool = pydantic.Field(
        False,
        title="Enable scheduled prediction submission",
        description="Uses the saved Collect LCC events configuration. Running manually also submits a due prediction.",
    )


class ListPredictionsConfig(ReferenceActionConfiguration):
    project_id: UUID
    model_id: Optional[UUID] = None
    limit: int = pydantic.Field(100, ge=1, le=1000)


class ResolveDeliveryConfig(GenericActionConfiguration, ExecutableActionMixin):
    batch_id: UUID
    outcome: Literal["confirmed_delivered", "confirmed_not_delivered"]
    gundi_object_ids: List[str] = pydantic.Field(
        default_factory=list,
        description="For confirmed delivery, the Gundi object IDs in outbox event order. Confirm the entire batch before resolving.",
    )
