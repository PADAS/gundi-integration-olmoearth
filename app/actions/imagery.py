"""Select dated Sentinel-2 evidence; never substitute an image across the change."""

import math
from datetime import datetime, timedelta, timezone

from shapely.geometry import box, shape

from app.services.errors import IntegrationBadResponseError

COLLECTION = "sentinel-2-l2a"


def thumbnail_area(event):
    west, south, east, north = shape(event["geometry"]["geometry"]).bounds
    # At least ~1 km across, with context around larger changes.
    lat = (south + north) / 2
    lon = (west + east) / 2
    dy = max((north - south) * .65, .0045)
    dx = max((east - west) * .65, .0045 / max(.05, math.cos(math.radians(lat))))
    bounds = [lon - dx, lat - dy, lon + dx, lat + dy]
    if not (-180 <= bounds[0] < bounds[2] <= 180 and -85 <= bounds[1] < bounds[3] <= 85):
        raise ValueError("Thumbnail bounds cross the supported map extent")
    return bounds


async def select_thumbnail(api, event, side, today):
    details = event["event_details"]
    value = details.get("ts_start" if side == "before" else "ts_end")
    if not value:
        return None
    # Dates are model estimates. Keep before/after on strictly opposite days.
    at = datetime.fromisoformat(value[:10]).replace(tzinfo=timezone.utc)
    if side == "before":
        start, end = at - timedelta(days=90), at - timedelta(microseconds=1)
    else:
        start, end = at + timedelta(days=1), at + timedelta(days=91)
    end = min(end, today)
    if start > end:
        return None
    bounds = thumbnail_area(event)
    region = box(*bounds)
    response = await api._request("POST", "/api/v1/datasets-imagery/scenes/search", {
        "collection": {"eq": COLLECTION},
        "intersects_geometry": region.__geo_interface__,
        "collected_at": {"gte": start.isoformat(), "lte": end.isoformat()},
        "cloud_cover": {"gte": 0, "lte": 20},
        "sort_by": "collected_at", "sort_direction": "desc" if side == "before" else "asc",
        "limit": 1000,
    }, attempts=1)
    records = response.get("records")
    if response.get("errors") or not isinstance(records, list):
        raise IntegrationBadResponseError("Invalid imagery scene search response")
    for scene in records:
        props = scene["properties"]
        # Avoid partially covered thumbnails. No cloud-free guarantee at pixel level.
        if shape(props["geometry"]).covers(region):
            date = props["collected_at"][:10]
            return {
                "filename": f"{side}-{date}-sentinel2.png",
                "query": {
                    "collection": COLLECTION, "bbox": bounds, "width": 512,
                    "filters": {"id": {"inc": [scene["id"]]}},
                    "render_config": {
                        "bands": ["B04", "B03", "B02"], "max_items": 1,
                        "color_formula": "Gamma RGB 3.5 Saturation 1.2 Sigmoidal RGB 15 0.35",
                    },
                },
            }
    return None
