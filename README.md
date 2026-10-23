# gundi-integration-olmoearth

Prototype of a scheduled LCC change monitor using the public APIs on OlmoEarth Studio
`develop`. It does **not** depend on Studio PR #3226 or a cross-result feature endpoint.
One saved Gundi integration is one area/model monitor; create another integration for another area.

## Configure the pilot

The Gundi action schemas generate the form. Use the integration's base URL for the
OlmoEarth environment and configure its Authenticate action with the API token.

1. Save **Collect LCC Events** (`pull_events`) using `examples/lcc-monitor.json` as a
   starting point. Every ID and the small example polygon are placeholders. Replace the
   project/model/organization, area, date-property name and aggregation through the form.
2. Choose mining or other `post_change` labels in the multi-select, or enable **Select all
   change types**. The enum is the current LCC legend, excluding nodata and `none`; verify
   it against the selected registered model before using a different model version.
3. Set the first prediction date and weekly/monthly/quarterly/annual frequency. Select a
   recent start date unless you intentionally want historical catch-up. Monthly/yearly
   slots retain the original day, clamping to the month's last day as needed.
4. Use a model configured to polygonize `post_change` with auxiliary properties and an
   actual ISO date field. Studio has no prediction-request vectorization flag. Raw
   timestep indices and encoded month counts are rejected, not guessed into dates.
5. Save **Submit LCC Predictions** (`submit_predictions`). Scheduled submission defaults
   to disabled; enable it when ready, or run the action manually to submit one due job.
   A manual action can incur OlmoEarth prediction charges.
6. Configure the Gundi route/EarthRanger event type and verify polygon rendering in the
   destination. The event carries both the polygon and a point guaranteed inside it.

The prediction API requires a project. A partner project is a useful default. Organization
is optional; when supplied it must own that project. API credentials still enforce access.
Neither IDs nor the OlmoEarth environment are hardcoded in the connector.

## Workflow

| Action | Schedule | Behavior |
| --- | --- | --- |
| `auth` | on demand | Verify token using authenticated prediction search. |
| `list_predictions` | reference lookup | List a project's predictions for inspection. |
| `submit_predictions` | every four hours, opt-in | Submit one due schedule slot if no job is in flight. |
| `pull_events` | every four hours, at :15 | Poll saved jobs, filter features, and deliver events. |
| `resolve_delivery` | manual only | Resolve a delivery whose acknowledgement was lost. |

Submission calls `POST /api/v1/predictions`, saves its ID, and collection polls
`GET /api/v1/predictions/{id}` until complete. Collection reads the actual nested
`result.id`, then pages `POST /api/v1/prediction-results/{result.id}/features/search`.
The area and selected post-change labels filter features. Opening EarthRanger does not
trigger inference: it displays events delivered through Gundi.

The model reference time is the schedule slot minus `imagery_lag_days` (14 by default;
verify for the chosen model). Start and end are both that reference time. The model's
own configuration determines its imagery lookbehind. Frequency does not set that
lookbehind or imply event-date precision. Annual sampling is not a promise of exhaustive
annual coverage. Missed slots catch up oldest first, with at most one job in flight.

## Dates and repeated detections

- `recorded_at` uses the verified polygon date property, encoded as UTC midnight.
  Preserve its month/day precision and aggregation in `event_details`; it is an estimate.
- `first_discovered_at` is when the connector found the polygon. A late-discovered change
  is retained even if its estimated date predates the last poll.
- `changes_since`, if set, is a fixed initial event-date cutoff. It never advances as a cursor.
- Selected classes and delivery settings are snapshotted per submitted job. Changing the
  class selection affects future jobs, not a half-read result or already delivered history.
- Project/model/area/schedule/date semantics changes require a new monitor integration,
  preserving old jobs instead of silently mixing identities.

Every new event gets a deterministic `event_details.external_event_id`. Gundi's
`external_source_id` identifies the monitor, **not** a per-event idempotency key. Store
Gundi's returned `object_id` separately after acknowledged delivery. See the
[Gundi API guide](https://support.earthranger.com/en_US/become-a-data-provider/gundi-api).

For consecutive predictions, the prototype suppresses a repeat when exactly one previously
known polygon has the same class, estimated date within `date_tolerance_days`, and polygon
intersection-over-union at least `overlap_threshold`. It records the new observation
locally; it does not PATCH the delivered EarthRanger event. Multiple matches create an event
with `ambiguous_match_count` for review. This is a regional, planar-overlap heuristic,
not a general change-tracking algorithm: inspect splits, merges, geometry drift and dates
in the pilot. Antimeridian-spanning AOIs are rejected. Geometry is selected by intersection,
not clipped to the AOI. Nearby separate polygons from the same prediction remain separate.

## Recovery and completeness

Redis holds the job ledger, per-result offset, event history and a delivery outbox under
`lcc_monitor_v1`. Configure durable Redis persistence/backups before a real deployment.
There is no prediction-creation or feature-time watermark: late-completing jobs remain
tracked. An action lock and shorter execution timeout prevent the two schedules from
writing this state concurrently.

Before submission the connector saves its exact request and unique name. It never retries
an ambiguous creation POST. The next submission action searches that name within the
project/model and recovers a single matching ID. Zero or multiple matches require an
operator to reconcile the saved job; they never cause an automatic second POST.

A page's payload is stored before delivery. The Gundi POST is attempted once. A successful
acknowledgement saves every returned object ID before committing the page offset. If the
outcome is unknown, the outbox stays `sending` and subsequent collection pauses rather
than potentially creating duplicates. **Resolve LCC Delivery** requires the exact batch ID:

- `confirmed_delivered`: supply each Gundi object ID in outbox order; the next collection
  commits the page without resending.
- `confirmed_not_delivered`: only after checking the entire batch, permit the next collection
  to send the saved payload again.
- Partial delivery requires manual reconciliation of the whole batch first. Do not mark it
  wholly undelivered. There is no claim of automatic exactly-once delivery.

The page budget pauses with a saved offset. Reaching Elasticsearch's 10,000-feature window
marks the job `needs_export` and blocks new submissions; it is never reported complete.
Use a smaller AOI for the prototype. Empty vector results complete normally; raster-only
results and malformed dates fail visibly. Upstream vectorization can itself remove/cap
polygons: use an uncapped model and inspect its sieve/simplification settings for the pilot.

History is bounded at 1,000 jobs and 10,000 known events and stops visibly at those limits.
It does not evict identities and silently resend old events. A durable indexed event store,
large-result export, and downstream event updates are follow-up work.

## Verification

Run `pytest` for the full suite, or `pytest app/actions/tests/test_olmoearth.py -q` for
connector contracts. Tests use mocked HTTP and state; no paid predictions or Gundi events
are created. They cover submission/reconciliation, real nested result IDs, out-of-order
completion, filtering, date validation, paging/resumption, interrupted delivery, receipt
validation, and repeat polygons across predictions.

For the live pilot, use one small AOI and mining, submit two successive reference dates,
check the actual date-property encoding, then inspect events and repeated polygons in
EarthRanger. Environment, credentials, project/model, actual AOI and the model's date
semantics must be provided before that validation. The older PR's cross-result polling
configuration is replaced by this form; re-register the integration actions and configure
it afresh rather than copying old watermark state.

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
