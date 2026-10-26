# Running an Action Runner Locally

You can run this Action Runner locally using Docker Compose. This will allow you to develop the code and debug it locally while still taking advantage of Gundi Core services.

## Requirements

- Python 3.10
- Docker
- Docker Compose

## Steps

Inside the `local` directory, do these things:

**Set your Environment**

Create a copy of `.env.local.example` and name it `.env.local`

Edit the `.env.local` file and set the `GUNDI_OAUTH_CLIENT_SECRET` to a secret from the stage environment (Ask the Gundi Team)

**Build and Run**

In a Terminal, run `docker compose up --build`

> [!NOTE]
>
> The Be sure to compile a `requirements.txt` file before running the docker compose command, or you might miss some important dependencies.


Once these steps are complete you can visit http://localhost:8080/docs to see your Action Runner's browsable API.

## Notes

- This example uses configuration from https://stage.gundiservice.org.





## Viewing the OlmoEarth prototype

This repository is the action runner, not the Gundi portal. Its `/docs` page is
an API explorer, not the partner configuration form. The form schema and UI
options come from `app/actions/configurations.py`; Gundi renders them after
integration-type registration. The optional Compose `web-ui` profile points to
sources that are not included, so leave that profile disabled.

There are four levels of validation:

| Goal | Required components |
| --- | --- |
| Exercise the monitor without accounts | `pytest app/actions/tests/test_olmoearth.py -q`; APIs, state, and delivery are mocked |
| Exercise the monitor and lock on real Redis | Start a disposable Redis, then `OLMOEARTH_TEST_REDIS_URL=redis://127.0.0.1:6390/15 pytest app/actions/tests -q` (the tests flush that database) |
| Browse the runner API | Python dependencies and `REGISTER_ON_START=false python -m uvicorn app.main:app --host 127.0.0.1 --port 8080`; open `/docs` |
| View the real form and deliver events | Gundi portal/API and OAuth, registered runner, Redis, scheduler/message delivery, OlmoEarth credentials/model, Gundi ingestion/routing and EarthRanger destination |

Starting the API alone does not start a scheduler. The schedule decorators
publish metadata during registration; Gundi must trigger the actions. The local
Compose emulator supports local message delivery, but does not provide Gundi's
portal, backend, identity provider, scheduler, or EarthRanger dispatcher. The
example environment connects to remote Gundi staging. Use a Gundi-provided
staging account and OAuth credentials rather than attempting to run those
services from this repository.

For a staging pilot:

1. Ask the Gundi team for staging portal access, runner OAuth credentials, and a
   reachable runner deployment URL. A remote Gundi service cannot call localhost.
2. Configure the environment from `.env.local.example`, then run Compose as above
   for development or deploy the runner with persistent Redis.
3. Register the runner using the same Gundi environment settings:
   `python -m app.register --slug olmoearth --name OlmoEarth --service-url <runner-url>`.
   Registration changes the integration type in Gundi; coordinate the slug with
   its owner. This publishes the action schemas and schedules.
4. In the portal, create a saved OlmoEarth integration, set its site/base URL to
   the chosen OlmoEarth environment, configure Authenticate, and save Collect
   LCC Events with the placeholder example replaced by actual pilot settings.
5. Configure the Gundi route and EarthRanger event type. Run Submit LCC Predictions
   once, then Collect LCC Events after the prediction completes. Enable scheduled
   submission only when the manual end-to-end run is verified.

For direct manual execution, POST `/v1/actions/execute` with
`{"integration_id":"<saved-gundi-integration-id>","action_id":"pull_events"}`.
Use `submit_predictions` to submit a due prediction. These are real operations:
monitor actions require a saved integration, Redis and credentials, and can
create predictions or send events. Merely opening `/docs` performs neither.
