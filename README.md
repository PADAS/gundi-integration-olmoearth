# gundi-integration-olmoearth
Gundi v2 connector for the OlmoEarth prediction API, built on the action-runner
template. The template's own documentation follows the connector section below.

## The OlmoEarth connector

Three actions, in `app/actions/`:

| Action | Type | What it does |
| --- | --- | --- |
| `auth` | auth | Validates the API token by asking the predictions search for one record. |
| `list_predictions` | reference | Populates a portal dropdown of predictions, newest first. |
| `pull_events` | pull (every 4h) | The ingest. Detections → Gundi events. |

### The ingest

One request per run, against the endpoint OlmoEarth added for external pollers
([olmoearth_studio#3226](https://github.com/allenai/olmoearth_studio/pull/3226)):

```
POST /api/v1/prediction-results/features/search    GeoJSON detections, paged
  -> Gundi events                                  one event per feature
```

The body has two halves and the split matters:

- **`prediction_results`** picks *which* Prediction Results to read — by model,
  project, organization or target area. This is the half OlmoEarth
  access-controls: it resolves the filters to the set of Results the token may
  read and scopes the feature query to it. `PullEventsConfig` requires at least
  one of those four, because an unscoped search returns every Result the token
  can see, and past a thousand Results the provider answers 400 rather than
  truncating.
- **`features`** filters the detections within those Results — the watermark,
  the area of interest, a confidence threshold — and carries the sort and
  paging.

`features.oe_prediction_result_id` is deliberately not a field on
`client.FeatureFilters`. The server derives that filter from
`prediction_results`, that derivation *is* the access control, and a
caller-supplied value is rejected with a 422. Leaving it off the model makes it
unreachable rather than merely discouraged.

Each feature becomes one Gundi event: `oe_start_time` is the `recorded_at`
(when the model saw the thing, not when the record was written), the geometry's
centre becomes the point `location`, the full GeoJSON geometry travels
alongside so a polygon detection stays a polygon, and the model's own
properties land in `event_details` with the `oe_` provenance fields renamed
rather than mixed in.

### Incremental runs

The watermark is `oe_created_at` on the features themselves, which arrives in
the same response as the detections. Three details keep it honest:

- The cursor is `gte`, not `gt`. A bulk-inserted Result stamps thousands of
  features with one `oe_created_at`; if some of them are indexed after a run
  has already read that second and moved past it, `gt` would never see them
  again. `gte` re-reads the boundary second every run.
- `boundary_feature_ids` is what stops that re-read becoming duplicate events:
  the ids already ingested at exactly the watermark second. It is bounded, and
  overflowing it re-sends detections rather than losing them — a duplicate
  `external_source_id` is recoverable downstream, a detection that was never
  sent is not.
- Those ids are *qualified* — `result-1:7`, the same string the event carries
  as its `external_source_id`. A bare feature id is unique only within its
  Prediction Result, and one search spans many at once, so keying on it would
  drop feature 1 of the second Result as a duplicate of feature 1 of the first.
- **A run never stops inside a second.** This is what keeps the ingest moving,
  and no size of boundary set can substitute for it. **Max Features Per Run**
  is a soft cap: on reaching it the run keeps taking detections until the
  timestamp changes, so the cursor always lands *between* groups. Stop
  mid-second instead and the cursor sits inside a group `gte` re-reads in full
  — and once that group is larger than the boundary set remembers, two runs
  trade halves of it forever and never reach what lies past it. The overrun is
  bounded by how many detections share one timestamp.
- A re-read does not count against the cap either. Only new detections do.
- Those ids are *qualified* — `result-1:7`, the same string the event carries
  as its `external_source_id`. A bare feature id is unique only within its
  Prediction Result, and one search now spans many at once, so keying on it
  would drop feature 1 of the second Result as a duplicate of feature 1 of the
  first.

State is saved after each batch of events, so a run that fails on the fifth
batch of six does not re-send the first four — and once more at the end
whatever the run did, because a run whose detections were all unusable still
read them and that progress is worth keeping. A run that changed nothing skips
the write entirely.

If paging hits its page ceiling (`client.MAX_PAGES`) with matches still unread,
the run reports `truncated` rather than ending quietly — otherwise a stuck feed
reads as a clean one.

Paging forces `sort_by=oe_created_at, sort_direction=asc` regardless of what
the caller asked for, because offset paging over a descending sort skips a
record whenever one is written mid-walk.

### The area of interest goes on the features, not the Results

`prediction_results.prediction_intersects_geometry` exists and looks like the
right place for it, but it matches through registered Areas only: a Prediction
created from an uploaded GeoJSON keeps its footprint in a file rather than an
Area row, so its detections never match. Enforcing an AOI there would silently
drop exactly the detections nobody would think to look for. The AOI is applied
to `features.intersects_geometry`, which is exact — and, now that the ingest is
a single request, free. **Target Area ID** is the separate, explicit setting
for pruning by registered Area.

### An empty run is ambiguous, so it is checked

The features endpoint is `optional_auth` on the provider's side: an invalid or
expired token does not 401, it degrades the caller to anonymous and returns
only the Results marked public. For a private feed that is zero detections and
no error — indistinguishable from a quiet day.

So a run that sent no events makes one extra request to
`/api/v1/predictions/search`, which does require a real user, before reporting
nothing new. A dead token fails the action as an auth problem instead of
looking like a quiet day for as long as nobody checks. The `auth` action
validates against that same endpoint, for the same reason.

## Usage
- Fork this repo
- Implement your own actions in `actions/handlers.py`
- Define configurations needed for your actions in `action/configurations.py`
- Or implement a webhooks handler in `webhooks/handlers.py`
- and define configurations needed for your webhooks in `webhooks/configurations.py`
- Optionally, add the `@activity_logger()` decorator in actions to log common events which you can later see in the portal:
    - Action execution started
    - Action execution complete
    - Error occurred during action execution
- Optionally, add the `@webhook_activity_logger()` decorator in the webhook handler to log common events which you can later see in the portal:
    - Webhook execution started
    - Webhook execution complete
    - Error occurred during webhook execution
- Optionally, use  `log_action_activity()` or `log_webhook_activity()` to log custom messages which you can later see in the portal
- Optionally, use  `@crontab_schedule()` or `register.py --schedule` to make an action to run on a custom schedule


## Action Examples: 

```python
# actions/configurations.py
from .core import PullActionConfiguration


class PullObservationsConfiguration(PullActionConfiguration):
    lookback_days: int = 10


```

```python
# actions/handlers.py
from app.services.activity_logger import activity_logger, log_activity
from app.services.gundi import send_observations_to_gundi
from app.services.utils import crontab_schedule
from gundi_core.events import LogLevel
from .configurations import PullObservationsConfiguration


@crontab_schedule("0 */4 * * *")  # Run every 4 hours
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfiguration):
    
    # Add your business logic to extract data here...
    
    # Optionally, log a custom messages to be shown in the portal
    await log_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        config_data=action_config.dict()
    )
    
    # Normalize the extracted data into a list of observations following to the Gundi schema:
    observations = [
        {
            "source": "collar-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 10
            }
        }
    ]
    
    # Send the extracted data to Gundi
    await send_observations_to_gundi(observations=observations, integration_id=integration.id)

    # The result will be recorded in the portal if using the activity_logger decorator
    return {"observations_extracted": 10}
```


## Webhooks Usage:
This framework provides a way to handle incoming webhooks from external services. You can define a handler function in `webhooks/handlers.py` and define the expected payload schema and configurations in `webhooks/configurations.py`. Several base classes are provided in `webhooks/core.py` to help you define the expected schema and configurations.


### Fixed Payload Schema
If you expect to receive data with a fixed schema, you can define a Pydantic model for the payload and configurations. These models will be used for validating and parsing the incoming data.
```python
# webhooks/configurations.py
import pydantic
from .core import WebhookPayload, WebhookConfiguration


class MyWebhookPayload(WebhookPayload):
    device_id: str
    timestamp: str
    lat: float
    lon: float
    speed_kmph: float


class MyWebhookConfig(WebhookConfiguration):
    custom_setting: str
    another_custom_setting: bool

```
### Webhook Handler
Your webhook handler function must be named webhook_handler and it must accept the payload and config as arguments. The payload will be validated and parsed using the annotated Pydantic model. The config will be validated and parsed using the annotated Pydantic model. You can then implement your business logic to extract the data and send it to Gundi.
```python
# webhooks/handlers.py
from app.services.activity_logger import webhook_activity_logger
from app.services.gundi import send_observations_to_gundi
from .configurations import MyWebhookPayload, MyWebhookConfig


@webhook_activity_logger()
async def webhook_handler(payload: MyWebhookPayload, integration=None, webhook_config: MyWebhookConfig = None):
    # Implement your custom logic to process the payload here...
    
    # If the request is related to an integration, you can use the integration object to access the integration's data
    
    # Normalize the extracted data into a list of observations following to the Gundi schema:
    transformed_data = [
        {
            "source": payload.device_id,
            "type": "tracking-device",
            "recorded_at": payload.timestamp,
            "location": {
                "lat": payload.lat,
                "lon": payload.lon
            },
            "additional": {
                "speed_kmph": payload.speed_kmph
            }
        }
    ]
    await send_observations_to_gundi(
          observations=transformed_data,
          integration_id=integration.id
      )
    
    return {"observations_extracted": 1}
```

### Dynamic Payload Schema
If you expect to receive data with different schemas, you can define a schema per integration using JSON schema. To do that, annotate the payload arg with the `GenericJsonPayload` model, and annotate the webhook_config arg with the `DynamicSchemaConfig` model or a subclass. Then you can define the schema in the Gundi portal, and the framework will build the Pydantic model on runtime based on that schema, to validate and parse the incoming data.
```python
# webhooks/configurations.py
import pydantic
from .core import DynamicSchemaConfig


class MyWebhookConfig(DynamicSchemaConfig):
    custom_setting: str
    another_custom_setting: bool

```
```python
# webhooks/handlers.py
from app.services.activity_logger import webhook_activity_logger
from .core import GenericJsonPayload
from .configurations import MyWebhookConfig


@webhook_activity_logger()
async def webhook_handler(payload: GenericJsonPayload, integration=None, webhook_config: MyWebhookConfig = None):
    # Implement your custom logic to process the payload here...
    return {"observations_extracted": 1}
```


### Simple JSON Transformations
For simple JSON to JSON transformations, you can use the [JQ language](https://jqlang.github.io/jq/manual/#basic-filters) to transform the incoming data. To do that, annotate the webhook_config arg with the `GenericJsonTransformConfig` model or a subclass. Then you can specify the `jq_filter` and the `output_type` (`ev` for event or `obv` for observation) in Gundi.
```python
# webhooks/configurations.py
import pydantic
from .core import WebhookPayload, GenericJsonTransformConfig


class MyWebhookPayload(WebhookPayload):
    device_id: str
    timestamp: str
    lat: float
    lon: float
    speed_kmph: float


class MyWebhookConfig(GenericJsonTransformConfig):
    custom_setting: str
    another_custom_setting: bool


```
```python
# webhooks/handlers.py
import json
import pyjq
from app.services.activity_logger import webhook_activity_logger
from app.services.gundi import send_observations_to_gundi
from .configurations import MyWebhookPayload, MyWebhookConfig


@webhook_activity_logger()
async def webhook_handler(payload: MyWebhookPayload, integration=None, webhook_config: MyWebhookConfig = None):
    # Sample implementation using the JQ language to transform the incoming data
    input_data = json.loads(payload.json())
    transformation_rules = webhook_config.jq_filter
    transformed_data = pyjq.all(transformation_rules, input_data)
    print(f"Transformed Data:\n: {transformed_data}")
    # webhook_config.output_type == "obv":
    response = await send_observations_to_gundi(
        observations=transformed_data,
        integration_id=integration.id
    )
    data_points_qty = len(transformed_data) if isinstance(transformed_data, list) else 1
    print(f"{data_points_qty} data point(s) sent to Gundi.")
    return {"data_points_qty": data_points_qty}
```


### Dynamic Payload Schema with JSON Transformations
You can combine the dynamic schema and JSON transformations by annotating the payload arg with the `GenericJsonPayload` model, and annotating the webhook_config arg with the `GenericJsonTransformConfig` models or their subclasses. Then you can define the schema and the JQ filter in the Gundi portal, and the framework will build the Pydantic model on runtime based on that schema, to validate and parse the incoming data, and apply a [JQ filter](https://jqlang.github.io/jq/manual/#basic-filters) to transform the data.
```python
# webhooks/handlers.py
import json
import pyjq
from app.services.activity_logger import webhook_activity_logger
from app.services.gundi import send_observations_to_gundi
from .core import GenericJsonPayload, GenericJsonTransformConfig


@webhook_activity_logger()
async def webhook_handler(payload: GenericJsonPayload, integration=None, webhook_config: GenericJsonTransformConfig = None):
    # Sample implementation using the JQ language to transform the incoming data
    input_data = json.loads(payload.json())
    filter_expression = webhook_config.jq_filter.replace("\n", ""). replace(" ", "")
    transformed_data = pyjq.all(filter_expression, input_data)
    print(f"Transformed Data:\n: {transformed_data}")
    # webhook_config.output_type == "obv":
    response = await send_observations_to_gundi(
        observations=transformed_data,
        integration_id=integration.id
    )
    data_points_qty = len(transformed_data) if isinstance(transformed_data, list) else 1
    print(f"{data_points_qty} data point(s) sent to Gundi.")
    return {"data_points_qty": data_points_qty}
```


### Hex string payloads
If you expect to receive payloads containing binary data encoded as hex strings (e.g. ), you can use StructHexString, HexStringPayload and HexStringConfig which facilitate validation and parsing of hex strings. The user will define the name of the field containing the hex string and will define the structure of the data in the hex string, using Gundi.
The fields are defined in the hex_format attribute of the configuration, following the [struct module format string syntax](https://docs.python.org/3/library/struct.html#format-strings). The fields will be extracted from the hex string and made available as sub-fields in the data field of the payload. THey will be extracted in the order they are defined in the hex_format attribute.
```python
# webhooks/configurations.py
from app.services.utils import StructHexString
from .core import HexStringConfig, WebhookConfiguration


# Expected data: {"device": "BF170A","data": "6881631900003c20020000c3", "time": "1638201313", "type": "bove"}
class MyWebhookPayload(HexStringPayload, WebhookPayload):
    device: str
    time: str
    type: str
    data: StructHexString

    
class MyWebhookConfig(HexStringConfig, WebhookConfiguration):
    custom_setting: str
    another_custom_setting: bool

"""
Sample configuration in Gundi:
{
    "hex_data_field": "data",
    "hex_format": {
        "byte_order": ">",
        "fields": [
            {
                "name": "start_bit",
                "format": "B",
                "output_type": "int"
            },
            {
                "name": "v",
                "format": "I"
            },
            {
                "name": "interval",
                "format": "H",
                "output_type": "int"
            },
            {
                "name": "meter_state_1",
                "format": "B"
            },
            {
                "name": "meter_state_2",
                "format": "B",
                "bit_fields": [
                    {
                        "name": "meter_batter_alarm",
                        "end_bit": 0,
                        "start_bit": 0,
                        "output_type": "bool"
                    },
                    {
                        "name": "empty_pipe_alarm",
                        "end_bit": 1,
                        "start_bit": 1,
                        "output_type": "bool"
                    },
                    {
                        "name": "reverse_flow_alarm",
                        "end_bit": 2,
                        "start_bit": 2,
                        "output_type": "bool"
                    },
                    {
                        "name": "over_range_alarm",
                        "end_bit": 3,
                        "start_bit": 3,
                        "output_type": "bool"
                    },
                    {
                        "name": "temp_alarm",
                        "end_bit": 4,
                        "start_bit": 4,
                        "output_type": "bool"
                    },
                    {
                        "name": "ee_error",
                        "end_bit": 5,
                        "start_bit": 5,
                        "output_type": "bool"
                    },
                    {
                        "name": "transduce_in_error",
                        "end_bit": 6,
                        "start_bit": 6,
                        "output_type": "bool"
                    },
                    {
                        "name": "transduce_out_error",
                        "end_bit": 7,
                        "start_bit": 7,
                        "output_type": "bool"
                    },
                    {
                        "name": "transduce_out_error",
                        "end_bit": 7,
                        "start_bit": 7,
                        "output_type": "bool"
                    }
                ]
            },
            {
                "name": "r1",
                "format": "B",
                "output_type": "int"
            },
            {
                "name": "r2",
                "format": "B",
                "output_type": "int"
            },
            {
                "name": "crc",
                "format": "B"
            }
        ]
    }
}
"""
# The data extracted from the hex string will be made available as new sub-fields as follows:
"""
{
    "device": "AB1234",
    "time": "1638201313",
    "type": "bove",
    "data": {
        "value": "6881631900003c20020000c3",
        "format_spec": ">BIHBBBBB",
        "unpacked_data": {
            "start_bit": 104,
            "v": 1663873,
            "interval": 15360,
            "meter_state_1": 32,
            "meter_state_2": 2,
            "r1": 0,
            "r2": 0,
            "crc": 195,
            "meter_batter_alarm": True,
            "empty_pipe_alarm": True,
            "reverse_flow_alarm": False,
            "over_range_alarm": False,
            "temp_alarm": False,
            "ee_error": False,
            "transduce_in_error": False,
            "transduce_out_error": False
        }
    }
}
"""
```
Notice: This can also be combined with Dynamic Schema and JSON Transformations. In that case the hex string will be parsed first, adn then the JQ filter can be applied to the extracted data.

### Custom UI for configurations (ui schema)
It's possible to customize how the forms for configurations are displayed in the Gundi portal. 
To do that, use `FieldWithUIOptions` in your models. The `UIOptions` and `GlobalUISchemaOptions` will allow you to customize the appearance of the fields in the portal by setting any of the ["ui schema"](https://rjsf-team.github.io/react-jsonschema-form/docs/api-reference/uiSchema) supported options.

```python
# Example
import pydantic
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions
from .core import AuthActionConfiguration, PullActionConfiguration


class AuthenticateConfig(AuthActionConfiguration):
    email: str  # This will be rendered with default widget and settings
    password: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        format="password",
        title="Password",
        description="Password for the Global Forest Watch account.",
        ui_options=UIOptions(
            widget="password",  # This will be rendered as a password input hiding the input
        )
    )
    ui_global_options = GlobalUISchemaOptions(
        order=["email", "password"],  # This will set the order of the fields in the form
    )


class MyPullActionConfiguration(PullActionConfiguration):
    lookback_days: int = FieldWithUIOptions(
        10,
        le=30,
        ge=1,
        title="Data lookback days",
        description="Number of days to look back for data.",
        ui_options=UIOptions(
            widget="range",  # This will be rendered ad a range slider
        )
    )
    force_fetch: bool = FieldWithUIOptions(
        False,
        title="Force fetch",
        description="Force fetch even if in a quiet period.",
        ui_options=UIOptions(
            widget="radio", # This will be rendered as a radio button
        )
    )
    ui_global_options = GlobalUISchemaOptions(
        order=[
            "lookback_days",
            "force_fetch",
        ],
    )
```
