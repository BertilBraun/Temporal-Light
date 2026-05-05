"""
Example client: demonstrates starting workflows, sending signals, and streaming results.

Run against a live API:
    API_URL=http://localhost:8080 python example/run_client.py
"""

import asyncio
import os

from temporal_light import Client, WorkflowFailedError

API_URL = os.environ.get("API_URL", "http://localhost:8080")


async def run_small_order() -> None:
    """Small order — no approval gate, completes after payment + receipt."""
    client = Client(API_URL)
    print("── Small order ($99) ──────────────────────────────")
    handle = await client.start("order_flow", order_id="order-001", amount=99.0)
    print(f"  started  {handle.workflow_id}")

    result = await handle.result()
    print(f"  result   {result}")


async def run_large_order_approved() -> None:
    """Large order — hits the approval gate, gets approved."""
    client = Client(API_URL)
    print("\n── Large order ($750) — approved ──────────────────")
    handle = await client.start("order_flow", order_id="order-002", amount=750.0)
    print(f"  started  {handle.workflow_id}")

    # Poll until the workflow is waiting for the signal, then approve.
    # In a real system the approval would come from a human via an external UI.
    print("  waiting for workflow to reach approval gate…")
    await asyncio.sleep(4)

    print("  sending approval signal…")
    await client.signal(
        handle.workflow_id,
        "approval",
        payload={"approved": True},
    )

    result = await handle.result()
    print(f"  result   {result}")


async def run_large_order_rejected() -> None:
    """Large order — hits the approval gate, gets rejected."""
    client = Client(API_URL)
    print("\n── Large order ($1 200) — rejected ────────────────")
    handle = await client.start("order_flow", order_id="order-003", amount=1200.0)
    print(f"  started  {handle.workflow_id}")

    await asyncio.sleep(4)

    print("  sending rejection signal…")
    await client.signal(
        handle.workflow_id,
        "approval",
        payload={"approved": False, "reason": "Budget ceiling exceeded."},
    )

    result = await handle.result()
    print(f"  result   {result}")


async def run_status_check() -> None:
    """Show how to check status without streaming the full result."""
    client = Client(API_URL)
    print("\n── Status check (non-blocking) ───────────────────")
    handle = await client.start("order_flow", order_id="order-004", amount=50.0)
    print(f"  started  {handle.workflow_id}")

    await asyncio.sleep(1)
    status = await handle.status()
    print(f"  status after 1 s: {status.value}")

    result = await handle.result()
    print(f"  result   {result}")


async def main() -> None:
    try:
        await run_small_order()
        await run_large_order_approved()
        await run_large_order_rejected()
        await run_status_check()
    except WorkflowFailedError as error:
        print(f"\n  WORKFLOW FAILED: {error}")


if __name__ == "__main__":
    asyncio.run(main())
