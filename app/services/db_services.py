import json
import logging
from pathlib import Path
import csv

import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import (
    JSON_DATA_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    USE_JSON_FALLBACK,
)

logger = logging.getLogger(__name__)
JSON_DATA_PATH = Path(JSON_DATA_DIR) / "users.json"


def ensure_postgres_config():
    if POSTGRES_PASSWORD or USE_JSON_FALLBACK:
        return

    raise RuntimeError(
        "POSTGRES_PASSWORD is not set. Add it to .env or export it before "
        "starting the Temporal worker, or set USE_JSON_FALLBACK=true."
    )


def get_connection(dbname=POSTGRES_DB):
    if USE_JSON_FALLBACK:
        raise RuntimeError("Postgres connection requested while JSON fallback is enabled")
    return psycopg2.connect(
        dbname=dbname,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        cursor_factory=RealDictCursor,
    )


def _load_json_users():
    if not JSON_DATA_PATH.exists():
        return []
    with JSON_DATA_PATH.open() as data_file:
        return json.load(data_file)


def _lookup_json_user(first_name, last_name, dob, address):
    for user in _load_json_users():
        if (
            user.get("first_name") == first_name
            and user.get("last_name") == last_name
            and user.get("dob") == dob
            and user.get("address") == address
        ):
            return user
    return None


def ensure_bank_schema():
    if USE_JSON_FALLBACK:
        from app.services.file_services import normalize_record

        Path(JSON_DATA_DIR).mkdir(parents=True, exist_ok=True)
        if not JSON_DATA_PATH.exists():
            sample_path = Path(__file__).resolve().parents[2] / "sample.csv"
            rows = []
            if sample_path.exists():
                with sample_path.open(newline="") as sample_file:
                    rows = [normalize_record(row) for row in csv.DictReader(sample_file)]
            JSON_DATA_PATH.write_text(json.dumps(rows, indent=2))
        return

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS held_amount NUMERIC(12, 2) NOT NULL DEFAULT 0")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS account_operations (
                    operation_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    cif_id TEXT NOT NULL,
                    amount NUMERIC(12, 2) NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id BIGSERIAL PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    operation_id TEXT,
                    row_number INTEGER,
                    cif_id TEXT,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )


def lookup_user(first_name, last_name, dob, address):
    if USE_JSON_FALLBACK:
        return _lookup_json_user(first_name, last_name, dob, address)

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT first_name, last_name, dob::text, address, cif_id, amount::text, held_amount::text
                FROM users
                WHERE first_name = %s
                  AND last_name = %s
                  AND dob = %s
                  AND address = %s
                LIMIT 1
                """,
                (first_name, last_name, dob, address),
            )
            result = cursor.fetchone()
    return dict(result) if result else None
