"""Example workflow: order processing with payment, approval gate, and receipt."""

import asyncio
import random

from temporal_light import activity, sleep, wait_for_signal, workflow


@activity(retries=3, timeout=30, backoff_seconds=5)
async def charge_payment(order_id: str, amount: float) -> dict:
    # Simulate occasional transient failures to demonstrate retries.
    if random.random() < 0.3:
        raise RuntimeError("Payment gateway timeout — will retry.")
    await asyncio.sleep(0.5)
    return {"transaction_id": f"txn_{order_id}_{int(amount * 100)}"}


@activity(retries=1, timeout=10, backoff_seconds=2)
async def send_receipt(order_id: str, transaction_id: str, amount: float) -> dict:
    await asyncio.sleep(0.2)
    print(f"[receipt] order={order_id} txn={transaction_id} amount={amount}")
    return {"sent": True}


@activity(retries=0, timeout=10, backoff_seconds=0)
async def cancel_order(order_id: str, reason: str) -> dict:
    await asyncio.sleep(0.1)
    print(f"[cancel] order={order_id} reason={reason}")
    return {"cancelled": True}


@workflow
async def order_flow(order_id: str, amount: float) -> dict:
    """Charge → wait for manager approval → send receipt (or cancel on rejection).

    Demonstrates: activity retries, sleep, and wait_for_signal.
    """
    payment_result = await charge_payment(order_id, amount)
    transaction_id: str = payment_result["transaction_id"]

    # For large orders, require explicit approval before fulfilling.
    if amount > 500:
        await sleep(minutes=1)  # brief pause to simulate review period
        approval = await wait_for_signal("approval")
        approved: bool = approval.get("approved", False)

        if not approved:
            await cancel_order(order_id, reason=approval.get("reason", "Rejected by manager."))
            return {"status": "cancelled", "order_id": order_id}

    receipt_result = await send_receipt(order_id, transaction_id, amount)
    return {
        "status": "completed",
        "order_id": order_id,
        "transaction_id": transaction_id,
        "receipt_sent": receipt_result["sent"],
    }
