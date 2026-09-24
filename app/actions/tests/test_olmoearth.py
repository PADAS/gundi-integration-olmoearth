"""Tests for the OlmoEarth connector.

The contracts worth pinning down here are the ones the API cannot enforce for
us: that an unset filter operator is *absent* from the request rather than sent
empty, that the connector never sends the Result scope the server derives for
itself, that offset paging cannot skip a record, that a polygon detection still
gets a point location, and that a failed run leaves a watermark the next run
can resume from without either dropping or duplicating detections.
"""
import datetime
import json

import httpx
import pytest
from gundi_core.schemas.v2 import Integration

import app.actions.client as client
from app.actions import handlers
from app.actions.configurations import (
    AuthenticateConfig,
    ListPredictionsConfig,
    PullEventsConfig,
)
from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationRateLimitError,
)

BASE_URL = "https://olmoearth.example.org"
PREDICTIONS_PATH = "/api/v1/predictions/search"
FEATURES_PATH = "/api/v1/prediction-results/features/search"


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
@pytest.fixture
def integration():
    return Integration.parse_obj(
        {
            "id": "bf1e4b30-6d1a-4c58-9a55-f2c1b0a9e111",
            "name": "OlmoEarth Test",
            "base_url": BASE_URL,
            "enabled": True,
            "type": {
                "id": "9a0c6f2a-2a55-4b1c-8f1e-0c2a4e5d6f70",
                "name": "OlmoEarth",
                "value": "olmoearth",
                "description": "OlmoEarth predictions",
                "actions": [],
            },
            "owner": {
                "id": "45018398-7a2a-4f48-8971-39a2710d5dbd",
                "name": "Gundi Engineering",
                "description": "Test organization",
            },
            "configurations": [
                {
                    "id": "0a6b4f3e-8b2c-4a55-9d21-7f5e4c3b2a19",
                    "integration": "bf1e4b30-6d1a-4c58-9a55-f2c1b0a9e111",
                    "action": {
                        "id": "3f0a1b2c-4d5e-6f70-8192-a3b4c5d6e7f8",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {"api_token": "s3cr3t-token"},
                }
            ],
            "additional": {},
            "default_route": None,
            "status": "healthy",
        }
    )


@pytest.fixture
def pull_config():
    return PullEventsConfig(model_id="model-abc")


def make_transport(routes):
    """A MockTransport dispatching by request path, recording every request.

    `routes` maps a path to either an httpx.Response or a list of responses
    served in order (for paging).
    """
    recorded = []

    def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        route = routes[request.url.path]
        if isinstance(route, list):
            return route.pop(0) if route else httpx.Response(200, json={"records": [], "meta": {"total": 0}})
        return route

    return httpx.MockTransport(handler), recorded


def json_response(records, total=None, errors=None, status=200):
    body = {"records": records, "meta": {"total": total if total is not None else len(records)}}
    if errors is not None:
        body["errors"] = errors
    return httpx.Response(status, json=body)


def feature(
    feature_id=1,
    geometry=None,
    bbox=None,
    start_time="2026-09-01T00:00:00Z",
    created_at="2026-09-02T00:00:00Z",
    result_id="result-1",
    extra_properties=None,
):
    properties = {
        "oe_start_time": start_time,
        "oe_created_at": created_at,
        "oe_prediction_result_id": result_id,
        "oe_prediction_result_file_id": "file-1",
    }
    properties.update(extra_properties or {})
    record = {
        "type": "Feature",
        "id": feature_id,
        "geometry": geometry or {"type": "Point", "coordinates": [-72.7, -51.7]},
        "properties": properties,
    }
    if bbox:
        record["bbox"] = bbox
    return record


def prediction(pid="pred-1", creation_time="2026-09-02T00:00:00Z", **extra):
    record = {
        "id": pid,
        "name": f"Run {pid}",
        "status": "completed",
        "model_id": "model-abc",
        "creation_time": creation_time,
    }
    record.update(extra)
    return record


def parsed(record):
    return client.Feature.parse_obj(record)


# --------------------------------------------------------------------------
# Request serialization
# --------------------------------------------------------------------------
def test_an_unset_operator_is_absent_from_the_body_not_sent_empty():
    """The documented sample shows every operator with an empty value. Sent
    literally, `{"eq": ""}` asks for records equal to the empty string — which
    matches nothing — so only the operators we set may appear."""
    body = client.PredictionSearchRequest(
        model_id=client.KeywordFilter(eq="model-abc")
    ).as_body()

    assert body["model_id"] == {"eq": "model-abc"}
    assert "neq" not in body["model_id"]
    assert "exists" not in body["model_id"]
    # Filters we never touched are absent entirely.
    assert "project_id" not in body
    assert "status" not in body


def test_a_feature_search_body_has_the_two_halves_the_endpoint_expects():
    """`prediction_results` picks the Results, `features` filters within them.
    Flattening the two into one object is the most likely way to get this
    endpoint wrong, so pin the shape."""
    body = client.FeatureSearchRequest(
        prediction_results=client.PredictionResultFilters(
            prediction_model_id=client.KeywordFilter(eq="model-abc")
        ),
        features=client.FeatureFilters(limit=250),
    ).as_body()

    assert body["prediction_results"] == {"prediction_model_id": {"eq": "model-abc"}}
    assert body["features"]["limit"] == 250
    assert body["features"]["sort_by"] == "oe_created_at"


def test_the_connector_cannot_send_the_result_scope_the_server_derives():
    """The server builds `features.oe_prediction_result_id` from the
    access-controlled `prediction_results` query and 422s a caller-supplied
    one. Leaving the field off the model is what makes that unreachable rather
    than merely discouraged."""
    assert "oe_prediction_result_id" not in client.FeatureFilters.__fields__

    with pytest.raises(ValueError):
        client.FeatureFilters(oe_prediction_result_id=client.KeywordFilter(eq="result-1"))


def test_datetimes_are_serialized_as_iso_strings():
    """`as_body` has to produce something json-serializable: httpx would choke
    on a datetime, and the failure would only show up against a live API."""
    when = datetime.datetime(2026, 9, 1, 12, 30, tzinfo=datetime.timezone.utc)
    body = client.PredictionSearchRequest(creation_time=client.DatetimeFilter(gte=when)).as_body()

    assert body["creation_time"] == {"gte": "2026-09-01T12:30:00+00:00"}
    json.dumps(body)  # would raise if anything were left un-encoded


def test_explicit_paging_values_survive_serialization():
    """limit/offset have defaults, so `exclude_defaults` would drop offset=0 —
    and an omitted offset is a different request from offset 0."""
    body = client.FeatureSearchRequest(features=client.FeatureFilters(limit=50, offset=0)).as_body()

    assert body["features"]["limit"] == 50
    assert body["features"]["offset"] == 0


# --------------------------------------------------------------------------
# Geometry
# --------------------------------------------------------------------------
def test_a_point_feature_keeps_its_own_coordinates():
    assert handlers.centroid_of(parsed(feature())) == {"lat": -51.7, "lon": -72.7}


def test_a_polygon_is_reduced_to_a_point_so_gundi_can_place_it():
    polygon = {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]],
    }
    location = handlers.centroid_of(parsed(feature(geometry=polygon)))

    assert location["lat"] == pytest.approx(0.8)
    assert location["lon"] == pytest.approx(0.8)


def test_the_providers_bbox_wins_over_averaging_the_vertices():
    """The provider computed the bbox over the real geometry; averaging the
    vertices of a ring double-counts the repeated closing point."""
    polygon = {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]],
    }
    location = handlers.centroid_of(parsed(feature(geometry=polygon, bbox=[0.0, 0.0, 2.0, 2.0])))

    assert location == {"lat": 1.0, "lon": 1.0}


def test_a_feature_with_no_geometry_has_no_location():
    record = feature()
    record["geometry"] = None
    assert handlers.centroid_of(parsed(record)) is None


# --------------------------------------------------------------------------
# Transformation
# --------------------------------------------------------------------------
def test_an_event_is_timestamped_when_the_model_saw_it_not_when_it_was_written(pull_config):
    """`oe_start_time` is the observation window; `oe_created_at` is when the
    record was written. They differ by however long processing took, which for
    a detection feed is the difference between a useful timestamp and a useless
    one."""
    event = handlers.transform_feature(
        parsed(feature(start_time="2026-08-01T00:00:00Z", created_at="2026-09-02T00:00:00Z")),
        pull_config,
    )

    assert event["recorded_at"].startswith("2026-08-01T00:00:00")
    assert event["event_details"]["detected_at"].startswith("2026-09-02T00:00:00")


def test_a_feature_without_a_start_time_falls_back_to_when_it_was_created(pull_config):
    record = feature()
    record["properties"].pop("oe_start_time")

    assert handlers.transform_feature(parsed(record), pull_config)["recorded_at"].startswith(
        "2026-09-02T00:00:00"
    )


def test_the_models_own_properties_are_kept_apart_from_the_oe_provenance(pull_config):
    """`oe_`-prefixed fields are bookkeeping. Burying the model's actual output
    among them is what makes a detection feed unreadable downstream."""
    event = handlers.transform_feature(
        parsed(feature(extra_properties={"confidence": 0.91, "species": "elephant"})), pull_config
    )

    details = event["event_details"]
    assert details["confidence"] == 0.91
    assert details["species"] == "elephant"
    assert not [key for key in details if key.startswith("oe_")]
    # Provenance is still present, under names that say what they are.
    assert details["prediction_result_id"] == "result-1"
    assert details["model_id"] == "model-abc"


def test_a_feature_id_is_qualified_by_its_result_so_it_is_globally_unique(pull_config):
    """Feature ids restart per Prediction Result — the sample response returns
    `1`. One search now spans many Results at once, which is precisely when an
    unqualified id would collide."""
    event = handlers.transform_feature(parsed(feature(feature_id=1, result_id="result-42")), pull_config)

    assert event["external_source_id"] == "result-42:1"


def test_a_feature_naming_no_result_still_gets_a_stable_id(pull_config):
    record = feature()
    record["properties"].pop("oe_prediction_result_id")

    assert handlers.transform_feature(parsed(record), pull_config)["external_source_id"] == "1"


def test_the_event_title_is_configurable_because_a_cross_result_feed_has_no_prediction_name(
    pull_config,
):
    pull_config.event_title_prefix = "Kaza deforestation"

    assert handlers.transform_feature(parsed(feature(3)), pull_config)["title"] == (
        "Kaza deforestation detection 3"
    )


def test_the_full_geometry_travels_with_the_event_by_default(pull_config):
    polygon = {"type": "Polygon", "coordinates": [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.0, 0.0]]]}
    event = handlers.transform_feature(parsed(feature(geometry=polygon)), pull_config)

    assert event["geometry"]["type"] == "Polygon"
    assert event["geometry"]["coordinates"] == polygon["coordinates"]


def test_geometry_can_be_left_off(pull_config):
    pull_config.include_geometry = False

    assert "geometry" not in handlers.transform_feature(parsed(feature()), pull_config)


def test_an_unplaceable_feature_is_skipped_rather_than_sent_without_a_location(pull_config):
    record = feature()
    record["geometry"] = None

    assert handlers.transform_feature(parsed(record), pull_config) is None


# --------------------------------------------------------------------------
# Query construction
# --------------------------------------------------------------------------
def since_of(request):
    return request.features.oe_created_at


def test_the_watermark_window_is_inclusive(pull_config):
    """`gt` would drop a detection written in the same second as the watermark
    but indexed after the run read it. The boundary-id list is what stops the
    re-read becoming a duplicate."""
    since = datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc)
    request = handlers.build_feature_search(pull_config, since)

    assert since_of(request).gte == since
    assert since_of(request).gt is None


def test_each_scope_field_lands_on_the_prediction_results_half(pull_config):
    """These are the filters the server access-controls. Putting one on the
    feature half instead would be accepted and quietly do nothing."""
    pull_config.project_id = "proj-1"
    pull_config.organization_id = "org-1"
    pull_config.target_area_id = "area-1"
    scope = handlers.build_feature_search(pull_config, datetime.datetime.now(datetime.timezone.utc)).prediction_results

    assert scope.prediction_model_id.eq == "model-abc"
    assert scope.prediction_project_id.eq == "proj-1"
    assert scope.organization_id.eq == "org-1"
    assert scope.prediction_target_area_id.eq == "area-1"


def test_a_minimum_confidence_becomes_a_provider_side_property_filter(pull_config):
    """Filtering here rather than after the fact is the difference between
    paging through every detection and paging through the ones that matter."""
    pull_config.min_confidence = 0.8
    features = handlers.build_feature_search(pull_config, datetime.datetime.now(datetime.timezone.utc)).features

    assert features.property_filters[0].property_name == "confidence"
    assert features.property_filters[0].numeric_filter.gte == 0.8


def test_the_confidence_property_name_is_configurable(pull_config):
    pull_config.min_confidence = 0.5
    pull_config.confidence_property = "score"
    request = handlers.build_feature_search(pull_config, datetime.datetime.now(datetime.timezone.utc))

    assert request.features.property_filters[0].property_name == "score"


def test_the_area_of_interest_filters_features_not_predictions(pull_config):
    """`prediction_intersects_geometry` matches through registered Areas only:
    a Prediction created from an uploaded GeoJSON keeps its footprint in a file
    and never matches. Enforcing an AOI there would silently drop exactly the
    detections nobody would think to look for."""
    pull_config.area_of_interest = {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]}
    request = handlers.build_feature_search(pull_config, datetime.datetime.now(datetime.timezone.utc))

    assert request.features.intersects_geometry.type == "Polygon"
    assert request.prediction_results.prediction_intersects_geometry is None


def test_a_malformed_area_of_interest_is_reported_as_a_configuration_problem(pull_config):
    """Classified as configuration, not as a provider failure: nothing was
    wrong upstream, and the operator needs to know it is their field."""
    pull_config.area_of_interest = {"coordinates": [[0, 0]]}  # no `type`

    with pytest.raises(IntegrationConfigurationError):
        handlers.build_feature_search(pull_config, datetime.datetime.now(datetime.timezone.utc))


def test_a_config_with_nothing_to_narrow_it_is_rejected_up_front():
    """Unscoped, the search returns every Result the token can read — a silent
    firehose, or a 400 past the provider's thousand-Result ceiling. Better
    found in the portal than at 4am."""
    with pytest.raises(ValueError, match="at least one"):
        PullEventsConfig()


@pytest.mark.parametrize(
    "scope",
    [{"model_id": "m"}, {"project_id": "p"}, {"organization_id": "o"}, {"target_area_id": "a"}],
)
def test_any_one_scope_is_enough(scope):
    assert PullEventsConfig(**scope)


# --------------------------------------------------------------------------
# Client transport
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_rejected_token_is_reported_as_an_auth_failure():
    transport, _ = make_transport({PREDICTIONS_PATH: httpx.Response(401, text="bad token")})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationAuthError):
            await api.search_predictions(client.PredictionSearchRequest())


@pytest.mark.asyncio
async def test_rate_limiting_is_classified_as_such_after_its_retries():
    transport, recorded = make_transport({PREDICTIONS_PATH: httpx.Response(429, text="slow down")})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationRateLimitError):
            await api.search_predictions(client.PredictionSearchRequest())
    assert len(recorded) > 1, "a 429 is worth retrying"


@pytest.mark.asyncio
async def test_a_bad_request_is_not_retried():
    """A 4xx is a definite answer. Retrying it just delays the error."""
    transport, recorded = make_transport({PREDICTIONS_PATH: httpx.Response(422, text="bad filter")})
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationBadResponseError):
            await api.search_predictions(client.PredictionSearchRequest())
    assert len(recorded) == 1


@pytest.mark.asyncio
async def test_too_broad_a_scope_is_reported_as_a_configuration_problem():
    """The provider refuses to search more than a thousand Prediction Results
    at once — it will not truncate, because a partial feature set is
    indistinguishable from a complete one. That is the integration's own
    filters being too wide, so say which ones narrow it."""
    transport, _ = make_transport(
        {
            FEATURES_PATH: httpx.Response(
                400, text="This search spans 4213 Prediction Results, more than the 1000 that can be searched at once."
            )
        }
    )
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationConfigurationError, match="Narrow it") as raised:
            await api.search_features(client.FeatureSearchRequest())

    # IntegrationConfigurationError is the one connector message forwarded
    # verbatim to the ephemeral caller, so it must not echo the provider's body.
    assert "4213" not in str(raised.value)


@pytest.mark.asyncio
async def test_errors_returned_alongside_a_200_with_no_records_are_a_failure():
    """The documented response carries `errors` next to `records`, so a 200 is
    not on its own proof that the search ran."""
    transport, _ = make_transport(
        {PREDICTIONS_PATH: json_response([], errors=[{"code": "not_found_error", "message": "no such model"}])}
    )
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        with pytest.raises(IntegrationBadResponseError, match="not_found_error"):
            await api.search_predictions(client.PredictionSearchRequest())


@pytest.mark.asyncio
async def test_partial_results_are_kept_when_errors_accompany_records():
    transport, _ = make_transport(
        {PREDICTIONS_PATH: json_response([prediction()], errors=[{"code": "partial", "message": "one shard down"}])}
    )
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        response = await api.search_predictions(client.PredictionSearchRequest())

    assert len(response.records) == 1


@pytest.mark.asyncio
async def test_the_token_is_sent_as_a_bearer_header():
    transport, recorded = make_transport({PREDICTIONS_PATH: json_response([])})
    async with client.OlmoEarthClient(BASE_URL, "s3cr3t", transport=transport) as api:
        await api.search_predictions(client.PredictionSearchRequest())

    assert recorded[0].headers["authorization"] == "Bearer s3cr3t"


def test_a_client_without_a_base_url_says_so_rather_than_building_a_broken_url():
    with pytest.raises(IntegrationConfigurationError, match="base URL"):
        client.OlmoEarthClient("", "token")


def test_a_client_without_a_token_points_at_the_auth_action():
    with pytest.raises(IntegrationConfigurationError, match="Authenticate"):
        client.OlmoEarthClient(BASE_URL, "")


# --------------------------------------------------------------------------
# Paging
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_paging_walks_every_page_until_a_short_one():
    transport, recorded = make_transport(
        {
            FEATURES_PATH: [
                json_response([feature(i) for i in range(3)], total=5),
                json_response([feature(3), feature(4)], total=5),
            ]
        }
    )
    request = client.FeatureSearchRequest(features=client.FeatureFilters(limit=3))
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        collected = [f async for f in api.iter_features(request)]

    # Ids come back as strings: the response sends integers, the filters send
    # keywords, and the model settles on one so identity is stable.
    assert [f.id for f in collected] == ["0", "1", "2", "3", "4"]
    assert [json.loads(r.content)["features"]["offset"] for r in recorded] == [0, 3]


@pytest.mark.asyncio
async def test_paging_sorts_ascending_so_a_new_record_cannot_shift_the_window():
    """Offset paging over a `desc` sort skips a record whenever one is written
    mid-walk: everything after it shifts one slot forward. Ascending order only
    ever appends past the window already read."""
    transport, recorded = make_transport({FEATURES_PATH: [json_response([feature(1)], total=1)]})
    request = client.FeatureSearchRequest(
        features=client.FeatureFilters(limit=50, sort_direction="desc")
    )
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        [f async for f in api.iter_features(request)]

    features = json.loads(recorded[0].content)["features"]
    assert features["sort_by"] == "oe_created_at"
    assert features["sort_direction"] == "asc"
    # The caller's own request object is left as they built it.
    assert request.features.sort_direction == "desc"


@pytest.mark.asyncio
async def test_paging_stops_once_the_reported_total_is_reached():
    """A provider that pads the last page to full size would otherwise be
    paged forever."""
    transport, recorded = make_transport(
        {FEATURES_PATH: [json_response([feature(i) for i in range(2)], total=2)] * 3}
    )
    request = client.FeatureSearchRequest(features=client.FeatureFilters(limit=2))
    async with client.OlmoEarthClient(BASE_URL, "t", transport=transport) as api:
        collected = [f async for f in api.iter_features(request)]

    assert len(collected) == 2
    assert len(recorded) == 1


# --------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------
@pytest.fixture
def captured_gundi(mocker):
    """Capture what the connector sends to Gundi instead of sending it."""
    sent = []

    async def capture(events, **kwargs):
        sent.append({"events": events, "integration_id": kwargs.get("integration_id")})
        return {"created": len(events)}

    mocker.patch.object(handlers.gundi_tools, "send_events_to_gundi", side_effect=capture)
    return sent


@pytest.fixture
def captured_state(mocker):
    """An in-memory stand-in for the Redis-backed state manager."""
    store = {}

    async def get_state(integration_id, action_id, source_id="no-source"):
        return store.get((integration_id, action_id, source_id), {})

    async def set_state(integration_id, action_id, state, source_id="no-source"):
        store[(integration_id, action_id, source_id)] = state

    mocker.patch.object(handlers.state_manager, "get_state", side_effect=get_state)
    mocker.patch.object(handlers.state_manager, "set_state", side_effect=set_state)
    return store


@pytest.fixture
def no_activity_logs(mocker):
    """The activity logger publishes to PubSub; the actions' own logic is what
    these tests are about."""
    async def noop(*args, **kwargs):
        return None

    mocker.patch.object(handlers, "log_action_activity", side_effect=noop)
    mocker.patch("app.services.activity_logger.publish_event", side_effect=noop)


def route_client(mocker, routes):
    """Point every client the handlers build at a MockTransport."""
    transport, recorded = make_transport(routes)
    real = handlers.client_for

    def build(integration, auth_config=None):
        api = real(integration, auth_config)
        api._client = httpx.AsyncClient(
            base_url=api.base_url,
            headers={"Authorization": f"Bearer {api._api_token}", "Content-Type": "application/json"},
            transport=transport,
        )
        return api

    mocker.patch.object(handlers, "client_for", side_effect=build)
    return recorded


def state_of(captured_state, integration):
    return captured_state[(str(integration.id), "pull_events", "no-source")]


@pytest.mark.asyncio
async def test_auth_checks_the_credentials_against_a_real_search(mocker, integration):
    """An empty result is still proof the host, path and token all work."""
    recorded = route_client(mocker, {PREDICTIONS_PATH: json_response([], total=0)})

    result = await handlers.action_auth(integration, AuthenticateConfig(api_token="s3cr3t-token"))

    assert result["valid_credentials"] is True
    assert json.loads(recorded[0].content)["limit"] == 1


@pytest.mark.asyncio
async def test_auth_asks_an_endpoint_that_actually_requires_a_token(mocker, integration):
    """The features endpoint accepts anonymous callers and answers 200 with the
    public Results, so validating against it would pass a dead token. The
    predictions search requires a real user."""
    recorded = route_client(mocker, {PREDICTIONS_PATH: json_response([], total=0)})

    await handlers.action_auth(integration, AuthenticateConfig(api_token="s3cr3t-token"))

    assert [r.url.path for r in recorded] == [PREDICTIONS_PATH]


@pytest.mark.asyncio
async def test_auth_surfaces_a_rejected_token(mocker, integration):
    route_client(mocker, {PREDICTIONS_PATH: httpx.Response(403, text="forbidden")})

    with pytest.raises(IntegrationAuthError):
        await handlers.action_auth(integration, AuthenticateConfig(api_token="wrong"))


@pytest.mark.asyncio
async def test_list_predictions_renders_options_the_portal_can_show(mocker, integration):
    route_client(
        mocker,
        {PREDICTIONS_PATH: json_response([prediction("pred-1"), prediction("pred-2")], total=2)},
    )

    response = await handlers.action_list_predictions(integration, ListPredictionsConfig())

    assert [option["value"] for option in response["options"]] == ["pred-1", "pred-2"]
    assert response["options"][0]["label"] == "Run pred-1"
    assert response["truncated"] is False


@pytest.mark.asyncio
async def test_list_predictions_says_when_the_list_is_only_a_prefix(mocker, integration):
    """The portal caches the list; it has to know it is not the whole set."""
    route_client(mocker, {PREDICTIONS_PATH: json_response([prediction("pred-1")], total=900)})

    response = await handlers.action_list_predictions(integration, ListPredictionsConfig(limit=1))

    assert response["truncated"] is True


@pytest.mark.asyncio
async def test_a_pull_turns_features_into_events(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    route_client(mocker, {FEATURES_PATH: [json_response([feature(1), feature(2)], total=2)]})

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["features_read"] == 2
    assert result["events_sent"] == 2
    events = captured_gundi[0]["events"]
    assert [e["external_source_id"] for e in events] == ["result-1:1", "result-1:2"]
    assert events[0]["event_type"] == "olmoearth_detection"


@pytest.mark.asyncio
async def test_a_pull_reads_every_result_in_one_request(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """The whole point of the new endpoint: detections from many Prediction
    Results arrive together, with no per-Result round trip."""
    recorded = route_client(
        mocker,
        {
            FEATURES_PATH: [
                json_response([feature(1, result_id="result-1"), feature(1, result_id="result-2")], total=2)
            ]
        },
    )

    result = await handlers.action_pull_events(integration, pull_config)

    assert len(recorded) == 1
    assert result["events_sent"] == 2
    assert [e["external_source_id"] for e in captured_gundi[0]["events"]] == ["result-1:1", "result-2:1"]


@pytest.mark.asyncio
async def test_a_detection_already_ingested_is_not_sent_again(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """The search is bounded with `gte`, so everything sharing the watermark
    second comes back every run. The boundary-id list is what keeps that from
    duplicating events."""
    routes = {FEATURES_PATH: [json_response([feature(1)], total=1)]}
    route_client(mocker, routes)
    await handlers.action_pull_events(integration, pull_config)

    # The same detection is still the newest thing the provider has.
    routes[FEATURES_PATH] = [json_response([feature(1)], total=1)]
    routes[PREDICTIONS_PATH] = json_response([], total=0)  # the empty-run token probe
    route_client(mocker, routes)
    second = await handlers.action_pull_events(integration, pull_config)

    assert second["features_read"] == 0
    assert second["features_skipped"] == 1
    assert len(captured_gundi) == 1, "nothing should have been re-sent"


@pytest.mark.asyncio
async def test_a_detection_written_into_the_boundary_second_is_still_picked_up(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """The reason the cursor is `gte` and not `gt`. A bulk-inserted Result
    stamps thousands of features with one `oe_created_at`; if some are indexed
    after a run has already read that second, `gt` would never see them."""
    routes = {FEATURES_PATH: [json_response([feature(1, created_at="2026-09-02T00:00:00Z")], total=1)]}
    route_client(mocker, routes)
    await handlers.action_pull_events(integration, pull_config)

    # Feature 2 lands in the same second, after the first run read it.
    routes[FEATURES_PATH] = [
        json_response(
            [
                feature(1, created_at="2026-09-02T00:00:00Z"),
                feature(2, created_at="2026-09-02T00:00:00Z"),
            ],
            total=2,
        )
    ]
    route_client(mocker, routes)
    second = await handlers.action_pull_events(integration, pull_config)

    assert second["features_skipped"] == 1  # feature 1, already sent
    assert second["events_sent"] == 1
    assert captured_gundi[1]["events"][0]["external_source_id"] == "result-1:2"


@pytest.mark.asyncio
async def test_the_watermark_advances_to_the_newest_detection(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker,
        {
            FEATURES_PATH: [
                json_response(
                    [
                        feature(1, created_at="2026-09-04T00:00:00Z"),
                        feature(2, created_at="2026-09-05T00:00:00Z"),
                    ],
                    total=2,
                )
            ]
        },
    )

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["watermark"].startswith("2026-09-05T00:00:00")
    state = state_of(captured_state, integration)
    # Only the newest instant's ids are remembered; everything older is
    # excluded by the cursor itself.
    assert state["boundary_feature_ids"] == ["result-1:2"]


@pytest.mark.asyncio
async def test_a_failure_partway_through_leaves_the_sent_batches_behind(
    mocker, integration, captured_state, no_activity_logs
):
    """State is written per batch, not once at the end. A run that dies on the
    second batch must not re-send the first one's events next time."""
    calls = []

    async def send(events, **kwargs):
        calls.append(events)
        if len(calls) == 2:
            raise RuntimeError("gundi is down")
        return {"created": len(events)}

    mocker.patch.object(handlers.gundi_tools, "send_events_to_gundi", side_effect=send)
    route_client(
        mocker,
        {
            FEATURES_PATH: [
                json_response(
                    [
                        feature(1, created_at="2026-09-04T00:00:00Z"),
                        feature(2, created_at="2026-09-05T00:00:00Z"),
                    ],
                    total=2,
                )
            ]
        },
    )

    with pytest.raises(RuntimeError):
        await handlers.action_pull_events(
            integration, PullEventsConfig(model_id="model-abc", events_per_request=1)
        )

    state = state_of(captured_state, integration)
    assert state["last_feature_created_at"].startswith("2026-09-04T00:00:00")
    assert state["boundary_feature_ids"] == ["result-1:1"]


@pytest.mark.asyncio
async def test_a_first_run_reaches_back_by_the_configured_lookback(
    mocker, integration, captured_gundi, captured_state, no_activity_logs
):
    recorded = route_client(
        mocker, {FEATURES_PATH: [json_response([], total=0)], PREDICTIONS_PATH: json_response([], total=0)}
    )

    await handlers.action_pull_events(
        integration, PullEventsConfig(model_id="model-abc", lookback_days=3)
    )

    body = json.loads(recorded[0].content)
    since = datetime.datetime.fromisoformat(body["features"]["oe_created_at"]["gte"])
    age = datetime.datetime.now(datetime.timezone.utc) - since
    assert 2.9 < age.total_seconds() / 86400 < 3.1


@pytest.mark.asyncio
async def test_events_are_sent_in_batches_of_the_configured_size(
    mocker, integration, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker, {FEATURES_PATH: [json_response([feature(i) for i in range(5)], total=5)]}
    )

    result = await handlers.action_pull_events(
        integration, PullEventsConfig(model_id="model-abc", events_per_request=2)
    )

    assert result["events_sent"] == 5
    assert [len(call["events"]) for call in captured_gundi] == [2, 2, 1]


@pytest.mark.asyncio
async def test_a_run_with_nothing_new_is_a_clean_no_op(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker, {FEATURES_PATH: [json_response([], total=0)], PREDICTIONS_PATH: json_response([], total=0)}
    )

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["features_read"] == 0
    assert result["events_sent"] == 0
    assert captured_gundi == []


@pytest.mark.asyncio
async def test_an_empty_run_proves_the_token_still_works_before_calling_it_quiet(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """The features endpoint accepts anonymous callers: an expired token gets a
    200 carrying only the public Results, which for a private feed is zero
    detections and no error. A run that found nothing has to rule that out."""
    recorded = route_client(
        mocker, {FEATURES_PATH: [json_response([], total=0)], PREDICTIONS_PATH: json_response([], total=0)}
    )

    await handlers.action_pull_events(integration, pull_config)

    assert [r.url.path for r in recorded] == [FEATURES_PATH, PREDICTIONS_PATH]


@pytest.mark.asyncio
async def test_an_empty_run_on_a_dead_token_fails_instead_of_looking_quiet(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker,
        {FEATURES_PATH: [json_response([], total=0)], PREDICTIONS_PATH: httpx.Response(401, text="expired")},
    )

    with pytest.raises(IntegrationAuthError):
        await handlers.action_pull_events(integration, pull_config)


@pytest.mark.asyncio
async def test_a_run_that_sent_events_does_not_pay_for_the_token_probe(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """Events arriving is itself proof the token reads private Results."""
    recorded = route_client(mocker, {FEATURES_PATH: [json_response([feature(1)], total=1)]})

    await handlers.action_pull_events(integration, pull_config)

    assert [r.url.path for r in recorded] == [FEATURES_PATH]


@pytest.mark.asyncio
async def test_unplaceable_features_are_counted_as_read_but_not_sent(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """`features_read` counts what the provider returned; `events_sent` counts
    what reached Gundi. Collapsing them would hide the skips."""
    placeless = feature(2)
    placeless["geometry"] = None
    route_client(mocker, {FEATURES_PATH: [json_response([feature(1), placeless], total=2)]})

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["features_read"] == 2
    assert result["events_sent"] == 1


@pytest.mark.asyncio
async def test_an_unplaceable_feature_still_advances_the_watermark(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """It will never become an event, so re-reading it every run forever is
    just work — and it would hold the cursor at its timestamp."""
    placeless = feature(2, created_at="2026-09-06T00:00:00Z")
    placeless["geometry"] = None
    route_client(
        mocker,
        {
            FEATURES_PATH: [json_response([placeless], total=1)],
            PREDICTIONS_PATH: json_response([], total=0),
        },
    )

    result = await handlers.action_pull_events(integration, pull_config)

    assert result["watermark"].startswith("2026-09-06T00:00:00")


@pytest.mark.asyncio
async def test_a_run_that_sends_nothing_still_persists_what_it_read(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """Progress is progress even when none of it became an event. Left
    unsaved, the same unplaceable features are re-read every run forever — and
    once there are `max_features_per_run` of them the ingest never gets past
    them at all."""
    placeless = feature(2, created_at="2026-09-06T00:00:00Z")
    placeless["geometry"] = None
    route_client(
        mocker,
        {
            FEATURES_PATH: [json_response([placeless], total=1)],
            PREDICTIONS_PATH: json_response([], total=0),
        },
    )

    await handlers.action_pull_events(integration, pull_config)

    state = state_of(captured_state, integration)
    assert state["last_feature_created_at"].startswith("2026-09-06T00:00:00")


@pytest.mark.asyncio
async def test_an_unparseable_stored_watermark_falls_back_to_the_lookback(
    mocker, integration, pull_config, captured_gundi, captured_state, no_activity_logs
):
    """A corrupt state row should degrade to "re-read the lookback window", not
    wedge the integration."""
    captured_state[(str(integration.id), "pull_events", "no-source")] = {
        "last_feature_created_at": "not-a-date",
        "boundary_feature_ids": [],
    }
    recorded = route_client(
        mocker, {FEATURES_PATH: [json_response([], total=0)], PREDICTIONS_PATH: json_response([], total=0)}
    )

    await handlers.action_pull_events(integration, pull_config)

    assert "gte" in json.loads(recorded[0].content)["features"]["oe_created_at"]


@pytest.mark.asyncio
async def test_an_integration_with_no_auth_configured_says_so(integration, pull_config):
    integration.configurations = []

    with pytest.raises(IntegrationConfigurationError, match="authentication"):
        handlers.client_for(integration)


# --------------------------------------------------------------------------
# Watermark bookkeeping
# --------------------------------------------------------------------------
def test_the_boundary_list_resets_when_the_cursor_moves_on():
    """Ids at an instant the cursor has passed are dead weight: `gte` on a
    later timestamp already excludes them."""
    watermark = handlers.Watermark()
    watermark.advance(parsed(feature(1, created_at="2026-09-01T00:00:00Z")))
    watermark.advance(parsed(feature(2, created_at="2026-09-01T00:00:00Z")))
    assert watermark.boundary_ids == ["result-1:1", "result-1:2"]

    watermark.advance(parsed(feature(3, created_at="2026-09-02T00:00:00Z")))
    assert watermark.boundary_ids == ["result-1:3"]


def test_overflowing_the_boundary_list_duplicates_rather_than_drops():
    """When one second holds more detections than we remember, the forgotten
    ids are re-read and re-sent next run. That is the direction to fail in: a
    duplicate `external_source_id` is recoverable downstream, a detection that
    was never sent is not."""
    watermark = handlers.Watermark(memory=3)
    for i in range(5):
        watermark.advance(parsed(feature(i, created_at="2026-09-01T00:00:00Z")))

    state = watermark.to_state()
    assert state["boundary_feature_ids"] == ["result-1:2", "result-1:3", "result-1:4"]

    # What the next run loads is the truncated list, so the forgotten ids come
    # back as new work rather than vanishing.
    resumed = handlers.Watermark(watermark.created_at, state["boundary_feature_ids"], memory=3)
    forgotten = parsed(feature(0, created_at="2026-09-01T00:00:00Z"))
    remembered = parsed(feature(4, created_at="2026-09-01T00:00:00Z"))
    assert resumed.already_ingested(forgotten) is False
    assert resumed.already_ingested(remembered) is True


def test_two_results_sharing_a_feature_id_are_two_detections():
    """Feature ids are unique within a Prediction Result, not across them, and
    one search now spans many Results at once. Keyed on the bare id, feature 1
    of result-2 would be mistaken for feature 1 of result-1 and dropped as
    already ingested — a detection lost to a numbering coincidence."""
    watermark = handlers.Watermark()
    first = parsed(feature(1, result_id="result-1"))
    second = parsed(feature(1, result_id="result-2"))

    watermark.advance(first)

    assert watermark.already_ingested(first) is True
    assert watermark.already_ingested(second) is False


def test_a_feature_with_no_created_at_cannot_move_the_cursor():
    """It is still sent — it just cannot advance a cursor defined in terms of a
    field it does not carry."""
    record = feature(1)
    record["properties"].pop("oe_created_at")
    watermark = handlers.Watermark()
    watermark.advance(parsed(record))

    assert watermark.created_at is None


# --------------------------------------------------------------------------
# Bounding boxes
# --------------------------------------------------------------------------
def test_a_three_dimensional_bbox_does_not_plot_altitude_as_longitude():
    """RFC 7946 §5 orders a bbox as every minimum then every maximum, so a 3-D
    box is [west, south, minAlt, east, north, maxAlt]. Read as a flat
    [minLon, minLat, maxLon, maxLat], the altitude becomes the longitude."""
    record = feature(geometry={"type": "Polygon", "coordinates": [[[0, 0], [0, 2], [2, 2], [0, 0]]]})
    record["bbox"] = [0.0, 0.0, 10.0, 2.0, 2.0, 20.0]  # 10m and 20m are altitudes

    assert handlers.centroid_of(parsed(record)) == {"lat": 1.0, "lon": 1.0}


def test_a_bbox_crossing_the_antimeridian_stays_on_its_own_side_of_the_globe():
    """§5.2 makes west > east legal for a box straddling 180°. Averaged as it
    stands it comes out at longitude 0 — the far side of the planet."""
    record = feature()
    record["bbox"] = [170.0, -10.0, -170.0, 10.0]

    assert handlers.centroid_of(parsed(record)) == {"lat": 0.0, "lon": 180.0}


def test_an_odd_length_bbox_is_ignored_rather_than_guessed_at():
    record = feature()
    record["bbox"] = [0.0, 0.0, 1.0, 1.0, 5.0]

    # Falls through to averaging the geometry's own positions.
    assert handlers.centroid_of(parsed(record)) == {"lat": -51.7, "lon": -72.7}


# --------------------------------------------------------------------------
# Features with no id
# --------------------------------------------------------------------------
def test_a_feature_with_no_id_is_skipped_rather_than_given_a_shared_identity(pull_config):
    """Everything downstream keys on external_source_id. With no id there is
    nothing stable to build one from, and every id-less feature in a Result
    would answer to the same one."""
    record = feature()
    record.pop("id")

    assert handlers.transform_feature(parsed(record), pull_config) is None


def test_two_id_less_features_do_not_suppress_each_other():
    """Registered under a shared placeholder, the second id-less feature at an
    instant looks like a repeat of the first and is counted as already
    ingested."""
    record = feature()
    record.pop("id")
    watermark = handlers.Watermark()
    watermark.advance(parsed(record))

    assert watermark.already_ingested(parsed(record)) is False
    assert watermark.boundary_ids == []


# --------------------------------------------------------------------------
# The run cap
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_run_stops_at_its_cap_and_carries_the_rest_in_the_watermark(
    mocker, integration, captured_gundi, captured_state, no_activity_logs
):
    route_client(
        mocker,
        {
            FEATURES_PATH: [
                json_response(
                    [feature(i, created_at="2026-09-0%dT00:00:00Z" % (i + 1)) for i in range(4)],
                    total=4,
                )
            ]
        },
    )

    result = await handlers.action_pull_events(
        integration, PullEventsConfig(model_id="model-abc", max_features_per_run=2)
    )

    assert result["features_read"] == 2
    assert result["watermark"].startswith("2026-09-02T00:00:00")


@pytest.mark.asyncio
async def test_re_reading_the_boundary_second_does_not_consume_the_run_cap(
    mocker, integration, captured_gundi, captured_state, no_activity_logs
):
    """A crowded watermark second comes back in full every run. Charged against
    the cap, those re-reads would fill a run on their own and the ingest would
    never reach the detections past them."""
    config = PullEventsConfig(model_id="model-abc", max_features_per_run=2)
    routes = {FEATURES_PATH: [json_response([feature(1), feature(2)], total=2)]}
    route_client(mocker, routes)
    await handlers.action_pull_events(integration, config)

    # Both come back (same second), plus one genuinely new detection.
    routes[FEATURES_PATH] = [
        json_response([feature(1), feature(2), feature(3, created_at="2026-09-03T00:00:00Z")], total=3)
    ]
    route_client(mocker, routes)
    second = await handlers.action_pull_events(integration, config)

    assert second["features_skipped"] == 2
    assert second["features_read"] == 1
    assert second["watermark"].startswith("2026-09-03T00:00:00")


def test_the_boundary_memory_is_never_smaller_than_a_runs_own_output():
    """Below that a run cannot remember what it just sent: the next run forgets
    the half it ingested, re-sends it, forgets the other half, and oscillates
    there forever without reaching the detections past the cap."""
    watermark = handlers.Watermark(memory=handlers.BOUNDARY_ID_MEMORY)
    for i in range(handlers.BOUNDARY_ID_MEMORY + 10):
        watermark.advance(parsed(feature(i, created_at="2026-09-01T00:00:00Z")))

    assert len(watermark.to_state()["boundary_feature_ids"]) == handlers.BOUNDARY_ID_MEMORY

    roomy = handlers.Watermark(memory=handlers.BOUNDARY_ID_MEMORY + 10)
    for i in range(handlers.BOUNDARY_ID_MEMORY + 10):
        roomy.advance(parsed(feature(i, created_at="2026-09-01T00:00:00Z")))

    assert len(roomy.to_state()["boundary_feature_ids"]) == handlers.BOUNDARY_ID_MEMORY + 10
