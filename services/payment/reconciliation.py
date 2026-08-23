"""Payment reconciliation worker.

Periodically finds payments stuck in PROCESSING — rows where Transaction 1
committed but Transaction 2 never ran, because the service died around the
gateway call. Kafka cannot recover these: the offset is committed either way,
and a redelivered message would short-circuit on the processed_events check.
Database state is the only remaining signal, so this worker polls for it.

Resolution re-derives the outcome from the payment_simulate directive persisted
in Transaction 1. The simulated gateway is a pure function of that directive,
so re-deriving is equivalent to querying it for the original result.

A real implementation would NOT do this. It would look the charge up at the
provider by payment_id, because a real gateway's outcome is not derivable —
only it knows whether the money actually moved. That lookup also distinguishes
PENDING (still in flight) and NOT_FOUND (never received) from a settled result,
states this simulation has no need to represent.
"""

import asyncio
import structlog
from asyncpg import Pool

from gateway import process_payment
from shared.events import wrap_event

logger = structlog.get_logger()

async def reconcile_stuck_payments(db_pool: Pool, stuck_duration: str = "3 minutes") -> int:
    """Finds payments stuck in PROCESSING and reconciles them.
    
    Returns:
        The number of payments successfully reconciled.
    """
    reconciled_count = 0
    
    async with db_pool.acquire() as conn:
        # 1. Find stuck payments
        stuck_payments = await conn.fetch(
            f"""
            SELECT id, order_id, amount, payment_simulate
            FROM payments
            WHERE status = 'PROCESSING'
              AND updated_at < NOW() - INTERVAL '{stuck_duration}'
            """
        )
        
        for payment in stuck_payments:
            payment_id = payment["id"]
            order_id = str(payment["order_id"])
            amount = payment["amount"]
            simulate_directive = payment["payment_simulate"]

            logger.info("reconciling_stuck_payment", payment_id=str(payment_id), order_id=order_id)

            # 2. Resolve the outcome using the directive persisted in Transaction 1,
            #    so a crashed "failure" order does not resolve to SUCCEEDED.
            success = await process_payment(amount, simulate_directive)
            
            new_status = "SUCCEEDED" if success else "FAILED"
            outbox_topic = "payment.succeeded" if success else "payment.failed"
            outbox_event = wrap_event(outbox_topic, {"order_id": order_id})
            
            # 3. Apply the resolution in a transaction
            async with conn.transaction():
                # Re-check status inside transaction to avoid race conditions
                current_status = await conn.fetchval(
                    "SELECT status FROM payments WHERE id = $1 FOR UPDATE", payment_id
                )
                
                if current_status != "PROCESSING":
                    # Someone else (or another relay instance) already fixed it
                    logger.info("payment_already_resolved", payment_id=str(payment_id))
                    continue
                    
                await conn.execute(
                    "UPDATE payments SET status = $1, updated_at = NOW() WHERE id = $2",
                    new_status, payment_id
                )
                
                await conn.execute(
                    """
                    INSERT INTO outbox_events (id, topic, key, payload, created_at)
                    VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                    """,
                    outbox_topic, order_id, outbox_event
                )
                
            reconciled_count += 1
            logger.info("stuck_payment_reconciled", payment_id=str(payment_id), new_status=new_status)
            
    return reconciled_count


async def run_reconciliation_worker(db_pool: Pool, shutdown_event: asyncio.Event, poll_interval: float = 60.0):
    """Background task to periodically reconcile stuck payments."""
    logger.info("reconciliation_worker_started")
    
    while not shutdown_event.is_set():
        try:
            await reconcile_stuck_payments(db_pool)
        except Exception:
            logger.exception("reconciliation_worker_error")
            
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=poll_interval)
        except asyncio.TimeoutError:
            pass
            
    logger.info("reconciliation_worker_stopped")
