# OlmoEarth, Gundi and EarthRanger — Current Workflow

Updated 2026-10-23. Describes the connector prototype in [PR #2](https://github.com/PADAS/gundi-integration-olmoearth/pull/2), not a verified live deployment.

## Which application am I looking at?

| Application | What it does | What we configure there |
| --- | --- | --- |
| OlmoEarth Studio/API | Owns projects, areas, models, predictions and vector results | Model/vectorization configuration, project access, reusable areas |
| Gundi portal/API | Configures integration instances, actions and delivery routes | OlmoEarth URL/token, project/model/AOI, classes, cadence, destination |
| Our Gundi action runner | Executes the monitor workflow | Python code and runner deployment settings |
| EarthRanger map/event UI | Shows events delivered to the destination | Inspect polygons, dates and event details |
| EarthRanger admin / event-type form builder | Defines the destination event types and fields | Event type identifier, fields and geometry/presentation settings to verify |
| Local runner `/docs` | API explorer for calling runner endpoints | Development inspection; it is not the Gundi partner form |

The supplied [admin link](https://olmoearth.dev.pamdas.org/admin/login/?next=%2Fadmin%2Fform-builder%2Fevent-types) identifies itself as **EarthRanger Administration (advanced view)**. Its `next` parameter points at the event-type form builder. The hostname includes OlmoEarth because it is the named EarthRanger development site; it is not the OlmoEarth prediction API or the Gundi portal. Only the login page was inspected. Existing event types, account permissions, routing and successful polygon rendering remain unverified.

There are two different forms: Gundi's integration form describes **what to monitor**; EarthRanger's event-type form describes **how the resulting event is structured/displayed**. The connector's `event_type` must agree with the destination mapping. Do not assume old notes about automatic event-type creation or point-versus-polygon behavior are verified for this site.

## Components and data flow

```mermaid
flowchart TD
    Partner[Partner] --> Portal[Gundi portal: monitor form]
    Portal --> Config[Gundi API and saved configuration]
    Scheduler[Gundi scheduler] --> Messages[Messaging / PubSub]
    Messages --> Runner[Our OlmoEarth action runner]
    Config --> Runner
    Runner <--> Redis[Redis: jobs, progress, known events, outbox]
    Runner -->|Create and poll predictions| OE[OlmoEarth API and inference]
    OE -->|Vector polygons and properties| Runner
    Runner -->|Send filtered events| Ingest[Gundi ingestion API]
    Ingest --> Route[Gundi routing and destination delivery]
    Route --> ER[EarthRanger map and event timeline]
    Admin[EarthRanger admin: event-type schema] -. configures .-> ER
```

These are logical responsibilities, not a verified deployment inventory. Gundi invokes the runner; merely starting the runner does not start its schedule. Opening EarthRanger displays delivered events and does not initiate inference.

## One monitoring cycle

1. Save one Gundi integration for one area/model. Configure the OlmoEarth base URL and token, project, optional organization ownership check, model, area geometry, selected `post_change` labels and cadence.
2. Gundi triggers Submit LCC Predictions every four hours when enabled. The runner checks whether the selected weekly/monthly/quarterly/annual slot is due. Scheduled submission defaults off.
3. Save the request in Redis, call `POST /api/v1/predictions`, then save the prediction ID. Use a model already configured for vectorization; there is no request-level vectorization flag.
4. Collect LCC Events polls `GET /api/v1/predictions/{id}`. Once complete, read the nested `result.id`.
5. Search `POST /api/v1/prediction-results/{result_id}/features/search` with AOI and class filters. Read date properties from the returned polygons and apply any fixed `changes_since` cutoff locally.
6. Match repeated detections, stage an event batch in Redis, submit it to Gundi, save ingestion receipt IDs, and advance the page checkpoint.
7. Gundi routes events to EarthRanger. An ingestion receipt does not prove final destination delivery; verify that separately.

The model reference time is the due slot minus an imagery lag (currently 14 days by default). Its own model configuration controls imagery lookbehind. Cadence is not the model's observation window or date precision.

## Change dates and repeated detections

The polygon comes from `post_change`; the change date is an auxiliary property of that polygon. The pilot requires a verified ISO date property and known aggregation/precision. Raw timestep indices and encoded month counts are rejected instead of guessed into dates.

- `recorded_at`: estimated occurrence date, represented as UTC midnight.
- `first_discovered_at`: when the connector found the detection.
- `changes_since`: optional fixed cutoff; never advances as a polling watermark.

A late-finishing prediction can therefore report an older change. Repeat matching uses class, date proximity and polygon overlap. This is a prototype heuristic, not proof that split/merged/drifting polygons are the same event. The code records repeat observations locally; updating or reversing an already delivered EarthRanger event is not implemented.

A saved outbox prevents blind retries after uncertain delivery. Partial acceptance or a lost acknowledgement requires manual reconciliation. Search is limited to the 10,000-feature window; reaching the limit stops explicitly. Redis needs durable storage/backups for live use.

## Reusable areas — agreed next step, not implemented

The partner should see the selected project's existing areas and choose one, or choose **Create new area**. They name and draw/upload the boundary, save it to the OlmoEarth project, and the newly returned area ID becomes available for future selections.

The desired connector contract is an area ID plus a resolved geometry snapshot for the prediction/filter boundary. The current code accepts geometry directly and snapshots the configuration per job. It does not yet list or create areas. Verify the Studio list/create endpoints, permissions and Gundi reference-field/map/upload support before implementing this flow. Define how a later edit to a saved boundary affects a monitor; do not silently mix boundaries with pending jobs.

## Credentials and configuration

| Value | Where it belongs | Current state |
| --- | --- | --- |
| OlmoEarth environment URL | Gundi integration site/base URL | Placeholder |
| OlmoEarth API token | Authenticate action; Bearer header on OlmoEarth requests | Placeholder |
| Project/model/optional organization/AOI | Collect LCC Events action | Placeholder examples |
| Gundi OAuth and API settings | Runner deployment/local environment | Example defaults and secret placeholder; confirm with Gundi team |
| EarthRanger destination credentials and mapping | Gundi route/destination | Not verified |
| EarthRanger admin login | EarthRanger website | User has a login; separate from API credentials above |

Do not put real secrets in notes or committed examples. An EarthRanger admin login alone does not supply Gundi OAuth or an OlmoEarth API token.

## Code layout and current implementation status

| File | Responsibility |
| --- | --- |
| `configurations.py` | Form fields and validation |
| `handlers.py` | Gundi action wiring |
| `client.py` | OlmoEarth HTTP calls and API schemas |
| `monitor.py` | Submission, polling, pagination and recovery |
| `monitor_state.py` | Persisted jobs, history, outbox and identity |
| `change_events.py` | Dates, polygon matching and event payloads |
| Shared `routers/actions.py` | Runner HTTP action endpoints |
| Shared services | Gundi authentication/configuration, Redis and ingestion |

The implementation is on `olmoearth-integration` in PR #2, based on the fork's `main` (the fork has no `develop`). The latest behavior-preserving refactor passed 418 local tests. This proves mocked contracts/workflows, not a live end-to-end deployment. No live prediction or event has been sent in this work. Studio PR #3226 and a new cross-result/events API are not prerequisites.

## Pilot completion criteria

Use one small mining AOI. Confirm credentials and date semantics; register/deploy the runner with Redis; configure the Gundi route and EarthRanger event type; submit a real prediction; inspect its polygons; collect; verify geometry, date and fields in EarthRanger. Run a second reference date to inspect repeat behavior. Only then enable recurring submission and validate cost, recovery and multiple cycles. Studio viewer links/thumbnails, destination updates and larger-scale storage/export remain follow-up work.
