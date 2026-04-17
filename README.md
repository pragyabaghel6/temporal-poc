# Temporal CSV Account Operations POC

## Overview
This repository demonstrates a production-style Temporal workflow for processing large government CSVs, validating records, checking bank accounts, and freezing or unfreezing account amounts.

## Features
- **ProcessCSVWorkflow**: Validates CSV structure, streams records in batches, checks DB, and applies account operations.
- **HoldAccountAmountWorkflow**: Manages hold/unhold lifecycle.
- **Activities**: Modular single-purpose functions for CSV read, validation, DB lookup, hold/unhold, and audit.
- **API**: FastAPI endpoints to start workflows and fetch status.
- **Retry behavior**: Transient activities retry up to 3 times. CSV/data validation is non-retryable.
- **Large files**: CSVs are read by row offset and batch size. The full file is never loaded into memory.
- **Fallback mode**: PostgreSQL is preferred. Set `USE_JSON_FALLBACK=true` for a local JSON-backed POC.

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create and seed the Postgres database:
   ```bash
   export POSTGRES_USER=postgres
   export POSTGRES_PASSWORD="<your-postgres-password>"
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432
   export POSTGRES_DB=finesdb

   venv/bin/python scripts/init_postgres.py
   ```

   The script creates the `finesdb` database, creates the `users` table, and seeds records from `sample.csv`.

3. Start Temporal locally, then run the worker:
   ```bash
   venv/bin/python -m app.worker
   ```

4. Run the FastAPI server:
   ```bash
   venv/bin/uvicorn app.main:app --reload
   ```

## JSON fallback
For a no-Postgres local run:
```bash
export USE_JSON_FALLBACK=true
venv/bin/python -m app.worker
```

The fallback seeds `data/users.json` from `sample.csv` the first time it runs and writes audit/operation state under `data/`.

## API
Start a workflow with a server-side file path:
```bash
curl -X POST http://localhost:8000/start \
  -H "Content-Type: application/json" \
  -d '{"file_path":"sample.csv","operation_type":"freeze"}'
```

Or upload a CSV:
```bash
curl -X POST http://localhost:8000/start \
  -F "file=@sample.csv" \
  -F "operation_type=unfreeze"
```

Check workflow status:
```bash
curl http://localhost:8000/workflow/<workflow_id>
```
