import csv
import sys
from pathlib import Path

import psycopg2
from psycopg2 import OperationalError
from psycopg2 import sql

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from app.services.file_services import normalize_record

from app.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


SAMPLE_CSV = ROOT_DIR / "sample.csv"


def connect(dbname):
    return psycopg2.connect(
        dbname=dbname,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def ensure_database():
    with connect("postgres") as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_DB,))
            if cursor.fetchone():
                return
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(POSTGRES_DB))
            )


def ensure_schema():
    with connect(POSTGRES_DB) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    dob DATE NOT NULL,
                    address TEXT NOT NULL,
                    cif_id TEXT NOT NULL,
                    amount NUMERIC(12, 2),
                    held_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (first_name, last_name, dob, address)
                )
                """
            )
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


def seed_sample_users():
    if not SAMPLE_CSV.exists():
        return

    with SAMPLE_CSV.open(newline="") as csv_file:
        rows = [normalize_record(row) for row in csv.DictReader(csv_file)]

    with connect(POSTGRES_DB) as conn:
        with conn.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO users (
                        first_name,
                        last_name,
                        dob,
                        address,
                        cif_id,
                        amount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (first_name, last_name, dob, address)
                    DO UPDATE SET
                        cif_id = EXCLUDED.cif_id,
                        amount = EXCLUDED.amount
                    """,
                    (
                        row["first_name"],
                        row["last_name"],
                        row["dob"],
                        row["address"],
                        row["cif_id"],
                        row["amount"],
                    ),
                )


def main():
    try:
        ensure_database()
        ensure_schema()
        seed_sample_users()
    except OperationalError as exc:
        raise SystemExit(
            "Could not connect to Postgres. Set POSTGRES_USER, "
            "POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, and try again.\n"
            f"Postgres error: {exc}"
        ) from exc

    print(f"Postgres database ready: {POSTGRES_DB}")


if __name__ == "__main__":
    main()
