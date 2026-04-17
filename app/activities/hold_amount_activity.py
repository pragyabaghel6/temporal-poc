import asyncio
import logging

from temporalio import activity

from app.services.bank_services import hold_amount, unhold_amount

logger = logging.getLogger(__name__)

@activity.defn
async def hold(record: dict):
    attempt = activity.info().attempt
    logger.info(
        "freeze activity row=%s cif_id=%s attempt=%s",
        record.get("_row_number"),
        record.get("cif_id"),
        attempt,
    )
    return await asyncio.to_thread(hold_amount, record)

@activity.defn
async def unhold(record: dict):
    attempt = activity.info().attempt
    logger.info(
        "unfreeze activity row=%s cif_id=%s attempt=%s",
        record.get("_row_number"),
        record.get("cif_id"),
        attempt,
    )
    return await asyncio.to_thread(unhold_amount, record)
