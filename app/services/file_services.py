import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path

DEFAULT_FINE_AMOUNT = "0"
REQUIRED_COLUMNS = {"first_name", "last_name", "dob", "address"}
REQUIRED_RECORD_FIELDS = REQUIRED_COLUMNS | {"cif_id", "amount"}


def normalize_record(record):
    normalized = {
        key.strip().lower(): (value.strip() if isinstance(value, str) else value)
        for key, value in record.items()
        if key is not None
    }

    if "cif_id" not in normalized and "cif_code" in normalized:
        normalized["cif_id"] = normalized["cif_code"]

    normalized.setdefault("amount", DEFAULT_FINE_AMOUNT)
    return normalized


def validate_csv_file(file_path):
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        raise ValueError(f"CSV file does not exist: {file_path}")

    with path.open(newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        headers = {header.strip().lower() for header in (reader.fieldnames or [])}

    missing = sorted(REQUIRED_COLUMNS - headers)
    has_cif = "cif_id" in headers or "cif_code" in headers
    if missing or not has_cif:
        if not has_cif:
            missing.append("cif_id or cif_code")
        raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")

    return {"file_path": str(path), "columns": sorted(headers)}


def read_csv_batch(file_path, start_row=0, batch_size=500):
    records = []
    next_row = start_row

    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_number, row in enumerate(reader):
            if row_number < start_row:
                continue
            if len(records) >= batch_size:
                break

            normalized = normalize_record(row)
            normalized["_row_number"] = row_number + 2
            records.append(normalized)
            next_row = row_number + 1

    return {
        "records": records,
        "next_row": next_row,
        "has_more": len(records) == batch_size,
    }


def validate_record(record):
    missing = [
        field for field in sorted(REQUIRED_RECORD_FIELDS) if not str(record.get(field, "")).strip()
    ]
    if missing:
        raise ValueError(
            f"row {record.get('_row_number', 'unknown')} is missing fields: "
            f"{', '.join(missing)}"
        )

    try:
        amount = Decimal(str(record["amount"]))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(
            f"row {record.get('_row_number', 'unknown')} has invalid amount"
        ) from exc

    if amount < 0:
        raise ValueError(f"row {record.get('_row_number', 'unknown')} has negative amount")

    record["amount"] = str(amount)
    return record


def validate_records(records):
    return [validate_record(record) for record in records]
