# Satellite thumbnails

Enable **Attach before and after satellite thumbnails** in Collect LCC Events before
submitting a new prediction. The setting is saved with each prediction; existing jobs
and events are not backfilled. It is off by default.

The connector uses the integration's Studio token to search Sentinel-2 L2A scenes
and render 512-pixel-wide true-color PNGs through Studio's datasets-imagery proxy.
It selects the nearest available scene strictly before the estimated change start
and strictly after the estimated change end, within 90 days, never beyond today.
Only scenes with at most 20% scene cloud cover and full thumbnail-area coverage are
eligible. A roughly 1 km minimum extent provides context. No eligible scene means
that side is skipped with an activity-log warning. Scene-level cloud cover does not
guarantee clear pixels at the event.

Images are named `before-YYYY-MM-DD-sentinel2.png` and
`after-YYYY-MM-DD-sentinel2.png` using acquisition dates. These are contextual
observations, not a claim that they were the model's exact input images. The images
do not include polygon overlays.

After Gundi acknowledges events, the connector saves attachment jobs using the
returned `object_id` and advances the event outbox. Each collection processes up to
four attachment sides, with a 90-second limit each; remaining sides wait for later
collections. Each side gets at most three attempts, then logs a warning and is
removed. Run additional collections to drain a large backlog; `thumbnails_pending`
reports its size. Event delivery continues independently of attachment failures.
Selected scene IDs are persisted before upload so retries use the same scene.

Delivery remains at least once: a crash after remote success but before saving state
can duplicate an event or attachment. The attachment API exposes no idempotency key;
filenames identify repeated sides. Normal attachment retries do not resend events.

## Live verification

Not yet tested against the live integration. Deploy this connector version, enable
the option, and submit a new prediction with a small AOI. Verify the event and its
before/after files in Gundi and EarthRanger, acquisition dates in filenames, and
activity logs for missing scenes or upload failures. Check that the integration token
can access both scene search and image rendering. No credentials belong in image URLs.
