# Pilot configuration placeholders

These files are examples to copy into the corresponding Gundi settings, not a
single importable integration or runtime defaults. Replace every placeholder
before running live actions. Keep real credentials in Gundi or your local
untracked environment file, not these examples.

| Setting | Example | Where to enter it |
| --- | --- | --- |
| OlmoEarth environment URL | `olmoearth-site.json` | Integration site/base URL |
| OlmoEarth API token | `olmoearth-auth.json` | Authenticate action |
| Project, optional organization, model, area, cadence, classes and date semantics | `lcc-monitor.json` | Collect LCC Events action |
| Gundi OAuth client ID/secret, issuer/audience and API URLs | `../local/.env.local.example` | Copy to `local/.env.local` for the runner |

The Gundi environment example contains staging URL/client defaults and a secret
placeholder. Confirm all of those values with the Gundi team for your chosen
environment. Gundi credentials authenticate the runner to Gundi; the OlmoEarth
token authenticates requests to OlmoEarth. They are separate credentials.

The all-zero-prefixed UUIDs and small polygon in `lcc-monitor.json` are sample
values, not actual partner resources. `event_type` must match the configured
Gundi/EarthRanger route. EarthRanger destination credentials belong in that Gundi
route/destination, not in this connector.

Save Submit LCC Predictions with `run_on_schedule` disabled until the pilot is
ready. The `run_on_schedule` field in the Collect configuration only controls
collection; it does not enable prediction submission.
