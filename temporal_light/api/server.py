"""FastAPI application: HTTP endpoints and SSE streaming."""

from __future__ import annotations

import os
import pathlib
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

_DASHBOARD_PATH = pathlib.Path(__file__).parent / 'dashboard.html'

from ..db import connection, queries
from ..models import WorkflowRecord, WorkflowStatus
from . import sse


@asynccontextmanager
async def _lifespan(app: FastAPI):  # type: ignore[type-arg]
    database_url = os.environ['DATABASE_URL']
    await connection.initialize_connection_pool(database_url)
    await sse.initialize_notify_listener(database_url)
    yield
    await sse.close_notify_listener()
    await connection.close_connection_pool()


app = FastAPI(title='Temporal-Light', lifespan=_lifespan)


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class StartWorkflowRequest(BaseModel):
    workflow_name: str
    workflow_input: dict[str, Any] = {}


class StartWorkflowResponse(BaseModel):
    workflow_id: str


class WorkflowStatusResponse(BaseModel):
    workflow_id: str
    name: str
    status: str
    created_at: str
    updated_at: str


class SendSignalRequest(BaseModel):
    signal_type: str
    payload: Any = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get('/', include_in_schema=False)
async def serve_dashboard() -> FileResponse:
    return FileResponse(_DASHBOARD_PATH, media_type='text/html')


@app.post('/workflows', response_model=StartWorkflowResponse, status_code=201)
async def start_workflow(request: StartWorkflowRequest) -> StartWorkflowResponse:
    workflow_id = str(uuid.uuid4())
    await queries.create_workflow(
        workflow_id=workflow_id,
        workflow_name=request.workflow_name,
        workflow_input=request.workflow_input,
    )
    return StartWorkflowResponse(workflow_id=workflow_id)


@app.get('/workflows/{workflow_id}', response_model=WorkflowStatusResponse)
async def get_workflow_status(workflow_id: str) -> WorkflowStatusResponse:
    workflow_record = await queries.get_workflow(workflow_id)
    if workflow_record is None:
        raise HTTPException(status_code=404, detail='Workflow not found.')
    return _workflow_record_to_response(workflow_record)


@app.get('/workflows/{workflow_id}/stream')
async def stream_workflow(workflow_id: str) -> StreamingResponse:
    workflow_record = await queries.get_workflow(workflow_id)
    if workflow_record is None:
        raise HTTPException(status_code=404, detail='Workflow not found.')

    existing_events = await queries.load_event_history(workflow_id)

    return StreamingResponse(
        sse.stream_workflow_events(workflow_id, existing_events),
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        },
    )


@app.post('/workflows/{workflow_id}/signals', status_code=204)
async def send_signal(workflow_id: str, request: SendSignalRequest) -> None:
    workflow_record = await queries.get_workflow(workflow_id)
    if workflow_record is None:
        raise HTTPException(status_code=404, detail='Workflow not found.')
    if workflow_record.status != WorkflowStatus.RUNNING:
        raise HTTPException(
            status_code=409,
            detail=f'Workflow is not running (status={workflow_record.status.value}).',
        )
    await queries.write_signal_and_wake_workflow(
        workflow_id=workflow_id,
        signal_type=request.signal_type,
        signal_payload=request.payload,
    )


@app.get('/workflows', response_model=list[WorkflowStatusResponse])
async def list_workflows(
    status: str | None = None,
    name: str | None = None,
    limit: int = 100,
) -> list[WorkflowStatusResponse]:
    """List workflows with optional filtering by status and/or name."""
    conditions = []
    parameters: list[Any] = []

    if status is not None:
        parameters.append(status)
        conditions.append(f'status = ${len(parameters)}')
    if name is not None:
        parameters.append(name)
        conditions.append(f'name = ${len(parameters)}')

    where_clause = f'WHERE {" AND ".join(conditions)}' if conditions else ''
    parameters.append(limit)

    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT * FROM workflows {where_clause} ORDER BY created_at DESC LIMIT ${len(parameters)}',
            *parameters,
        )
    return [_workflow_record_to_response(queries._row_to_workflow_record(row)) for row in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _workflow_record_to_response(record: WorkflowRecord) -> WorkflowStatusResponse:
    return WorkflowStatusResponse(
        workflow_id=record.workflow_id,
        name=record.name,
        status=record.status.value,
        created_at=record.created_at.isoformat(),
        updated_at=record.updated_at.isoformat(),
    )
