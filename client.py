import asyncio
from temporalio.client import Client
from app.config import TEMPORAL_SERVER, TASK_QUEUE
from app.workflows.revenue_file_workflow import RevenueFileWorkflow

async def main():
    client = await Client.connect(TEMPORAL_SERVER)
    handle = await client.start_workflow(
        RevenueFileWorkflow.run,
        "sample.csv",
        id="revenue-file-workflow-001",
        task_queue=TASK_QUEUE,
    )
    print(f"Started workflow {handle.id}")

if __name__ == "__main__":
    asyncio.run(main())
