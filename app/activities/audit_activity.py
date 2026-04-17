import asyncio
import json
import logging
from pathlib import Path

from temporalio import activity

from app.config import JSON_DATA_DIR, USE_JSON_FALLBACK
from app.services.db_services import get_connection

logger = logging.getLogger(__name__)
JSON_AUDIT_PATH = Path(JSON_DATA_DIR) / "audit_logs.json"


def _write_audit(event: dict):
    if USE_JSON_FALLBACK:
        Path(JSON_DATA_DIR).mkdir(parents=True, exist_ok=True)
        logs = []
        if JSON_AUDIT_PATH.exists():
            logs = json.loads(JSON_AUDIT_PATH.read_text())
        logs.append(event)
        JSON_AUDIT_PATH.write_text(json.dumps(logs, indent=2))
        return True

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO audit_logs (
                    workflow_id,
                    operation_id,
                    row_number,
                    cif_id,
                    status,
                    message
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    event["workflow_id"],
                    event.get("operation_id"),
                    event.get("row_number"),
                    event.get("cif_id"),
                    event["status"],
                    event.get("message"),
                ),
            )
    return True


@activity.defn
async def write(event: dict):
    attempt = activity.info().attempt
    logger.info(
        "audit event workflow_id=%s row=%s status=%s attempt=%s",
        event.get("workflow_id"),
        event.get("row_number"),
        event.get("status"),
        attempt,
    )
    return await asyncio.to_thread(_write_audit, event)
