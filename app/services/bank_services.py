import json
import logging
from decimal import Decimal
from pathlib import Path

from app.config import JSON_DATA_DIR, USE_JSON_FALLBACK
from app.services.db_services import ensure_bank_schema, get_connection

logger = logging.getLogger(__name__)
JSON_STATE_PATH = Path(JSON_DATA_DIR) / "account_operations.json"


def _operation_id(record, operation_type):
    return record.get("operation_id") or (
        f"{record['workflow_id']}:{operation_type}:row-{record['_row_number']}"
    )


def _json_apply(record, operation_type):
    Path(JSON_DATA_DIR).mkdir(parents=True, exist_ok=True)
    operations = []
    if JSON_STATE_PATH.exists():
        operations = json.loads(JSON_STATE_PATH.read_text())

    operation_id = _operation_id(record, operation_type)
    for operation in operations:
        if operation["operation_id"] == operation_id:
            return {**operation, "duplicate": True}

    operation = {
        "operation_id": operation_id,
        "workflow_id": record["workflow_id"],
        "operation_type": operation_type,
        "cif_id": record["cif_id"],
        "amount": str(record["amount"]),
        "status": "applied",
    }
    operations.append(operation)
    JSON_STATE_PATH.write_text(json.dumps(operations, indent=2))
    return {**operation, "duplicate": False}


def _postgres_apply(record, operation_type):
    ensure_bank_schema()
    operation_id = _operation_id(record, operation_type)
    amount = Decimal(str(record["amount"]))

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status
                FROM account_operations
                WHERE operation_id = %s
                """,
                (operation_id,),
            )
            existing = cursor.fetchone()
            if existing:
                return {
                    "operation_id": operation_id,
                    "operation_type": operation_type,
                    "cif_id": record["cif_id"],
                    "amount": str(amount),
                    "status": existing["status"],
                    "duplicate": True,
                }

            if operation_type == "freeze":
                cursor.execute(
                    """
                    UPDATE users
                    SET held_amount = held_amount + %s
                    WHERE cif_id = %s
                    """,
                    (amount, record["cif_id"]),
                )
            else:
                cursor.execute(
                    """
                    UPDATE users
                    SET held_amount = GREATEST(held_amount - %s, 0)
                    WHERE cif_id = %s
                    """,
                    (amount, record["cif_id"]),
                )

            cursor.execute(
                """
                INSERT INTO account_operations (
                    operation_id,
                    workflow_id,
                    operation_type,
                    cif_id,
                    amount,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    operation_id,
                    record["workflow_id"],
                    operation_type,
                    record["cif_id"],
                    amount,
                    "applied",
                ),
            )

    return {
        "operation_id": operation_id,
        "operation_type": operation_type,
        "cif_id": record["cif_id"],
        "amount": str(amount),
        "status": "applied",
        "duplicate": False,
    }


def apply_account_operation(record, operation_type):
    logger.info(
        "applying account operation type=%s cif_id=%s workflow_id=%s row=%s",
        operation_type,
        record.get("cif_id"),
        record.get("workflow_id"),
        record.get("_row_number"),
    )
    if USE_JSON_FALLBACK:
        return _json_apply(record, operation_type)
    return _postgres_apply(record, operation_type)


def hold_amount(record):
    return apply_account_operation(record, "freeze")


def unhold_amount(record):
    return apply_account_operation(record, "unfreeze")
