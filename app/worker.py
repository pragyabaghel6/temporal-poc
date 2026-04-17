import asyncio
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from temporalio.worker import Worker
from temporalio.client import Client
from app.config import TEMPORAL_SERVER, TASK_QUEUE
from app.services.db_services import ensure_bank_schema, ensure_postgres_config
from app.workflows.revenue_file_workflow import ProcessCSVWorkflow
from app.workflows.hold_account_amount_workflow import HoldAccountAmountWorkflow


from app.activities import (
    csv_read_activity,
    file_validation_activity,
    postgres_lookup_activity,
    hold_amount_activity,
    audit_activity,
)

async def main():
    logging.basicConfig(level=logging.INFO)
    try:
        ensure_postgres_config()
        await asyncio.to_thread(ensure_bank_schema)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    client = await Client.connect(TEMPORAL_SERVER)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[ProcessCSVWorkflow, HoldAccountAmountWorkflow],
        activities=[
            csv_read_activity.read_csv_activity,
            file_validation_activity.validate,
            postgres_lookup_activity.lookup,
            hold_amount_activity.hold,
            hold_amount_activity.unhold,
            audit_activity.write,
        ],
    )
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
