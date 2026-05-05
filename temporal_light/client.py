"""Client SDK: start workflows, stream results, send signals."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from .models import WorkflowStatus

_SSE_MAX_ATTEMPTS = 3
_SSE_BACKOFF_BASE_SECONDS = 1.0


class WorkflowHandle:
    """Reference to a running or completed workflow instance.

    Returned by Client.start(). Holds the workflow_id and a back-reference
    to the client for status and result calls. No DB connection is opened
    until result() is called.
    """

    def __init__(self, client: Client, workflow_id: str) -> None:
        self.client = client
        self.workflow_id = workflow_id

    async def status(self) -> WorkflowStatus:
        """Poll the current workflow status via GET /workflows/{id}."""
        async with httpx.AsyncClient() as http_client:
            response = await http_client.get(
                f"{self.client.base_url}/workflows/{self.workflow_id}"
            )
            response.raise_for_status()
            data = response.json()
            return WorkflowStatus(data["status"])

    async def result(self) -> Any:
        """Stream events via SSE until the workflow completes, then return its result.

        Retries up to _SSE_MAX_ATTEMPTS times with exponential backoff on connection
        errors. Raises WorkflowFailedError if the workflow failed or all retries
        are exhausted.
        """
        last_error: Exception | None = None
        for attempt in range(_SSE_MAX_ATTEMPTS):
            if attempt > 0:
                await asyncio.sleep(_SSE_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
            try:
                terminal = await self._stream_until_terminal()
                if terminal is not None:
                    return terminal
            except (
                httpx.RemoteProtocolError,
                httpx.ConnectError,
                httpx.ReadError,
            ) as connection_error:
                last_error = connection_error

        raise WorkflowFailedError(
            workflow_id=self.workflow_id,
            error=(
                f"SSE stream failed after {_SSE_MAX_ATTEMPTS} attempts: {last_error}"
            ),
        )

    async def _stream_until_terminal(self) -> Any:
        """Open one SSE connection and read until a terminal event or stream close.

        Returns the workflow result on workflow_completed. Raises WorkflowFailedError
        on workflow_failed. Returns None if the stream closes without a terminal event
        (caller should retry).
        """
        async with httpx.AsyncClient(timeout=None) as http_client:
            async with http_client.stream(
                "GET",
                f"{self.client.base_url}/workflows/{self.workflow_id}/stream",
            ) as response:
                response.raise_for_status()
                async for raw_line in response.aiter_lines():
                    if not raw_line.startswith("data:"):
                        continue
                    event_data = json.loads(raw_line[len("data:"):].strip())
                    event_type = event_data.get("type")

                    if event_type == "workflow_completed":
                        return event_data.get("result")

                    if event_type == "workflow_failed":
                        raise WorkflowFailedError(
                            workflow_id=self.workflow_id,
                            error=event_data.get("error", "Unknown error"),
                        )
        return None


class Client:
    """HTTP client for the Temporal-Light API.

    Usage::

        client = Client("http://api:8080")
        handle = await client.start("order_flow", order_id="123", amount=99.0)
        result = await handle.result()
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def start(self, workflow_name: str, **workflow_input: Any) -> WorkflowHandle:
        """Start a workflow by name and return a handle to it."""
        async with httpx.AsyncClient() as http_client:
            response = await http_client.post(
                f"{self.base_url}/workflows",
                json={"workflow_name": workflow_name, "workflow_input": workflow_input},
            )
            response.raise_for_status()
            data = response.json()
            return WorkflowHandle(client=self, workflow_id=data["workflow_id"])

    async def signal(
        self,
        workflow_id: str,
        signal_type: str,
        payload: Any = None,
    ) -> None:
        """Send a named signal to a running workflow."""
        async with httpx.AsyncClient() as http_client:
            response = await http_client.post(
                f"{self.base_url}/workflows/{workflow_id}/signals",
                json={"signal_type": signal_type, "payload": payload},
            )
            response.raise_for_status()


class WorkflowFailedError(Exception):
    def __init__(self, workflow_id: str, error: str) -> None:
        self.workflow_id = workflow_id
        self.error = error
        super().__init__(f"Workflow {workflow_id} failed: {error}")
