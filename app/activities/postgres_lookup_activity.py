import asyncio
import logging

from temporalio import activity

from app.services.db_services import lookup_user

logger = logging.getLogger(__name__)

@activity.defn
async def lookup(record: dict):
    attempt = activity.info().attempt
    logger.info(
        "looking up account row=%s cif_id=%s attempt=%s",
        record.get("_row_number"),
        record.get("cif_id"),
        attempt,
    )
    return await asyncio.to_thread(
        lookup_user,
        record["first_name"],
        record["last_name"],
        record["dob"],
        record["address"],
    )
