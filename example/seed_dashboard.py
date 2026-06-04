"""Populate the database with completed, failed, and nested workflows for the dashboard.

Drives the real WorkflowRunner against DATABASE_URL (defaults to the local docker
Postgres), so children are spawned with proper parent_id links and the dashboard's
hierarchical view has real trees to show.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid

# Run from the repo root even when launched as `python example/seed_dashboard.py`:
# otherwise sys.path[0] is example/ and `import temporal_light` resolves to any
# editable install elsewhere on the machine instead of this checkout.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from temporal_light import activity, spawn_child, wait_for_child, workflow
from temporal_light.db import connection
from temporal_light.db.queries import claim_next_workflow, create_workflow, register_worker
from temporal_light.worker.runner import WorkflowRunner


@activity(retries=2, timeout=10, backoff_seconds=0)
async def compute_score(label: str) -> dict:
    return {'label': label, 'score': len(label) * 7}


@workflow
async def greet_flow(name: str) -> str:
    return f'hello {name}'


@workflow
async def member_flow(member: str) -> dict:
    return await compute_score(member)


@workflow
async def team_flow(team: str, size: int) -> dict:
    member_ids = [await spawn_child('member_flow', member=f'{team}-m{index}') for index in range(size)]
    scores = [(await wait_for_child(member_id))['score'] for member_id in member_ids]
    return {'team': team, 'total': sum(scores)}


@workflow
async def org_flow(org: str) -> dict:
    team_ids = [await spawn_child('team_flow', team=f'{org}-t{index}', size=2) for index in range(2)]
    totals = [(await wait_for_child(team_id))['total'] for team_id in team_ids]
    return {'org': org, 'grand_total': sum(totals)}


@workflow
async def pipeline_flow(steps: int) -> list:
    task_ids = [await spawn_child('member_flow', member=f'task-{index}') for index in range(steps)]
    return [await wait_for_child(task_id) for task_id in task_ids]


@workflow
async def doomed_flow() -> None:
    raise RuntimeError('intentional failure for the dashboard demo')


WORKFLOW_REGISTRY = {
    'greet_flow': greet_flow,
    'member_flow': member_flow,
    'team_flow': team_flow,
    'org_flow': org_flow,
    'pipeline_flow': pipeline_flow,
    'doomed_flow': doomed_flow,
}


async def drain(runner: WorkflowRunner, worker_id: str) -> int:
    runs = 0
    while True:
        record = await claim_next_workflow(worker_id)
        if record is None:
            return runs
        await runner.run_workflow(record)
        runs += 1


async def main() -> None:
    database_url = os.environ.get('DATABASE_URL', 'postgresql://tl:changeme@localhost:5432/temporal_light')
    await connection.initialize_connection_pool(database_url)
    worker_id = 'seed-worker'
    await register_worker(worker_id)
    runner = WorkflowRunner(WORKFLOW_REGISTRY)

    roots: list[tuple[str, dict]] = (
        [('greet_flow', {'name': f'user-{index}'}) for index in range(4)]
        + [('pipeline_flow', {'steps': 3}) for _ in range(3)]
        + [('org_flow', {'org': f'org-{index}'}) for index in range(2)]
        + [('doomed_flow', {})]
    )
    for name, workflow_input in roots:
        await create_workflow(str(uuid.uuid4()), name, workflow_input)

    runs = await drain(runner, worker_id)
    print(f'created {len(roots)} root workflows; executed {runs} runs total')
    await connection.close_connection_pool()


if __name__ == '__main__':
    asyncio.run(main())
