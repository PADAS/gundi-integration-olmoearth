"""Delivery of a monitor's outbox batches to Gundi."""

from typing import Optional

import stamina

from app.services.errors import IntegrationBadResponseError
from app.services.gundi import (  # app.settings loads before gundi_client_v2
    GUNDI_API_RETRY,
    GundiClient,
    GundiDataSenderClient,
)


@stamina.retry(**GUNDI_API_RETRY)
async def get_gundi_client(integration_id: str) -> GundiDataSenderClient:
    """The integration's Gundi sender, looked up with the template's retry policy.

    ``send_events_to_gundi`` also retries the POST inside one run; the monitor posts
    once per run and leaves resending to its outbox.
    """
    async with GundiClient() as gundi:
        api_key = await gundi.get_integration_api_key(integration_id=str(integration_id))
    if not api_key:
        raise IntegrationBadResponseError(
            f"Gundi returned no API key for integration {integration_id}."
        )
    return GundiDataSenderClient(integration_api_key=api_key)


class GundiEventSender:
    """Posts a batch to Gundi once per call, reusing the client for the run."""

    def __init__(self, integration_id: str):
        self.integration_id = integration_id
        self._client: Optional[GundiDataSenderClient] = None

    async def send(self, events: list[dict]) -> list[dict]:
        """POST the batch once; raise unless Gundi acknowledged every event."""
        if self._client is None:
            self._client = await get_gundi_client(self.integration_id)
        response = await self._client.post_events(data=events)
        if (
            not isinstance(response, list)
            or len(response) != len(events)
            or any(not isinstance(item, dict) or item.get("errors") for item in response)
        ):
            raise IntegrationBadResponseError(
                "Gundi did not acknowledge every event; the batch will be sent again."
            )

        return response

    async def attach(self, event_id: str, filename: str, data: bytes) -> None:
        if self._client is None:
            self._client = await get_gundi_client(self.integration_id)
        response = await self._client.post_event_attachments(
            event_id=event_id, attachments=[(filename, data)]
        )
        if (not isinstance(response, list) or len(response) != 1
                or not isinstance(response[0], dict) or response[0].get("errors")
                or not response[0].get("object_id")):
            raise IntegrationBadResponseError("Gundi did not acknowledge the thumbnail")
