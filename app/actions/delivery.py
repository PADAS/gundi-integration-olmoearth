"""One-shot delivery of a monitor's outbox batches to Gundi."""

from typing import Optional

from gundi_client_v2.client import GundiDataSenderClient

from app.services.errors import IntegrationBadResponseError
from app.services.gundi import get_gundi_sender


class GundiEventSender:
    """Posts each batch exactly once; its owner reconciles ambiguous delivery.

    ``prepare`` resolves the integration's Gundi client (retried, and cached for
    the run) before the caller marks a batch as sending, so a failure there
    provably sent nothing.
    """

    def __init__(self, integration_id: str):
        self.integration_id = integration_id
        self._client: Optional[GundiDataSenderClient] = None

    async def prepare(self) -> None:
        if self._client is None:
            self._client = await get_gundi_sender(self.integration_id)

    async def post(self, events: list[dict]) -> None:
        """POST the batch once; raise unless Gundi acknowledged every event."""
        response = await self._client.post_events(data=events)
        if (
            not isinstance(response, list)
            or len(response) != len(events)
            or any(not isinstance(item, dict) or item.get("errors") for item in response)
        ):
            raise IntegrationBadResponseError(
                "Gundi did not acknowledge every event; reconcile the outbox before retrying."
            )
