import asyncio
import logging

from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.services.file_services import validate_csv_file, validate_records

logger = logging.getLogger(__name__)

@activity.defn
async def validate(input_data):
    attempt = activity.info().attempt
    logger.info("validating csv data attempt=%s", attempt)
    try:
        if isinstance(input_data, dict) and "file_path" in input_data:
            return await asyncio.to_thread(validate_csv_file, input_data["file_path"])
        return await asyncio.to_thread(validate_records, input_data)
    except ValueError as exc:
        raise ApplicationError(
            str(exc),
            type="VALIDATION_ERROR",
            non_retryable=True,
        ) from exc
