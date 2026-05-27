"""Example workflow: order processing with child workflow, approval gate, and receipt."""

import asyncio
import random

from temporal_light import activity, sleep, spawn_child, wait_for_child, wait_for_signal, workflow


@activity(retries=3, timeout=30, backoff_seconds=5)
async def charge_payment(order_id: str, amount: float) -> dict:
    # Simulate occasional transient failures to demonstrate retries.
    if random.random() < 0.5:
        raise RuntimeError('Payment gateway timeout - will retry.')
    await asyncio.sleep(0.5)
    return {'transaction_id': f'txn_{order_id}_{int(amount * 100)}'}


@activity(retries=1, timeout=10, backoff_seconds=2)
async def send_receipt(order_id: str, transaction_id: str, amount: float) -> dict:
    await asyncio.sleep(0.2)
    print(f'[receipt] order={order_id} txn={transaction_id} amount={amount}')
    return {'sent': True}


@activity(retries=0, timeout=10, backoff_seconds=0)
async def cancel_order(order_id: str, reason: str) -> dict:
    await asyncio.sleep(0.1)
    print(f'[cancel] order={order_id} reason={reason}')
    return {'cancelled': True}


@workflow
async def order_flow(order_id: str, amount: float) -> dict:
    """Charge, spawn risk check, wait for approval, then send receipt.

    Demonstrates: activity retries, child workflows, sleep, and wait_for_signal.
    """
    payment_result = await charge_payment(order_id, amount)
    transaction_id: str = payment_result['transaction_id']

    # The child starts immediately and runs independently while the parent
    # continues through any approval delay.
    risk_check_id = await spawn_child('risk_check_flow', order_id=order_id, amount=amount)

    # For large orders, require explicit approval before fulfilling.
    if amount > 500:
        await sleep(seconds=10)  # brief pause to simulate review period
        approval = await wait_for_signal('approval')
        approved: bool = approval.get('approved', False)

        if not approved:
            await cancel_order(order_id, reason=approval.get('reason', 'Rejected by manager.'))
            return {'status': 'cancelled', 'order_id': order_id}

    risk_result = await wait_for_child(risk_check_id)
    if risk_result['risk'] == 'high':
        await cancel_order(order_id, reason='Risk check failed.')
        return {'status': 'cancelled', 'order_id': order_id, 'risk': risk_result}

    receipt_result = await send_receipt(order_id, transaction_id, amount)
    return {
        'status': 'completed',
        'order_id': order_id,
        'transaction_id': transaction_id,
        'receipt_sent': receipt_result['sent'],
        'risk': risk_result,
    }


@workflow
async def risk_check_flow(order_id: str, amount: float) -> dict:
    """Independent child workflow used by order_flow."""
    await sleep(seconds=1)
    return {
        'order_id': order_id,
        'risk': 'high' if amount >= 5000 else 'low',
    }
