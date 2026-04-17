import asyncio
import logging

from temporalio import activity

from app.services.file_services import read_csv_batch

logger = logging.getLogger(__name__)

@activity.defn
async def read_csv_activity(input_data: dict):
    attempt = activity.info().attempt
    file_path = input_data["file_path"]
    start_row = int(input_data.get("start_row", 0))
    batch_size = int(input_data.get("batch_size", 500))
    logger.info(
        "reading csv batch file=%s start_row=%s batch_size=%s attempt=%s",
        file_path,
        start_row,
        batch_size,
        attempt,
    )
    return await asyncio.to_thread(read_csv_batch, file_path, start_row, batch_size)
