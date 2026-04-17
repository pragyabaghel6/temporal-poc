import logging
import asyncio
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, UploadFile
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError

from app.config import BATCH_SIZE, TASK_QUEUE, TEMPORAL_SERVER, UPLOAD_DIR
from app.models import OperationType, StartWorkflowRequest
from app.workflows.revenue_file_workflow import ProcessCSVWorkflow

logger = logging.getLogger(__name__)

app = FastAPI(title="Temporal CSV Account Operations POC")


async def _get_temporal_client() -> Client:
    client = getattr(app.state, "temporal_client", None)
    if client is None:
        client = await Client.connect(TEMPORAL_SERVER)
        app.state.temporal_client = client
    return client


async def _save_upload(file: UploadFile) -> str:
    upload_dir = Path(UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(file.filename or "upload.csv").suffix or ".csv"
    target = upload_dir / f"{uuid4()}{suffix}"

    with target.open("wb") as output:
        while chunk := await file.read(1024 * 1024):
            await asyncio.to_thread(output.write, chunk)

    return str(target)


async def _parse_start_request(request: Request) -> StartWorkflowRequest:
    content_type = request.headers.get("content-type", "")

    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        operation_type = form.get("operation_type")
        upload = form.get("file")
        file_path = form.get("file_path")

        if hasattr(upload, "read") and hasattr(upload, "filename"):
            file_path = await _save_upload(upload)

        if not file_path or not operation_type:
            raise HTTPException(
                status_code=422,
                detail="multipart request requires operation_type and file or file_path",
            )

        return StartWorkflowRequest(
            file_path=str(file_path),
            operation_type=OperationType(str(operation_type)),
        )

    try:
        payload = await request.json()
        return StartWorkflowRequest(**payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc


@app.post("/start")
async def start_workflow(request: Request):
    try:
        start_request = await _parse_start_request(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    workflow_id = f"process-csv-{start_request.operation_type.value}-{uuid4()}"
    input_data = {
        "file_path": start_request.file_path,
        "operation_type": start_request.operation_type.value,
        "batch_size": BATCH_SIZE,
    }

    try:
        client = await _get_temporal_client()
        handle = await client.start_workflow(
            ProcessCSVWorkflow.run,
            input_data,
            id=workflow_id,
            task_queue=TASK_QUEUE,
        )
    except Exception as exc:
        logger.exception("failed to start workflow")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    logger.info("started workflow_id=%s input=%s", handle.id, input_data)
    return {"workflow_id": handle.id}


@app.get("/workflow/{workflow_id}")
async def get_workflow_status(workflow_id: str):
    try:
        client = await _get_temporal_client()
        handle = client.get_workflow_handle(workflow_id)
        description = await handle.describe()
    except Exception as exc:
        logger.exception("failed to describe workflow_id=%s", workflow_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    status = description.status.name if description.status else "UNKNOWN"
    response = {
        "workflow_id": workflow_id,
        "status": status,
        "result": None,
        "error": None,
    }

    if description.status == WorkflowExecutionStatus.COMPLETED:
        response["result"] = await handle.result()
    elif description.status in {
        WorkflowExecutionStatus.FAILED,
        WorkflowExecutionStatus.CANCELED,
        WorkflowExecutionStatus.TERMINATED,
        WorkflowExecutionStatus.TIMED_OUT,
    }:
        try:
            await handle.result()
        except WorkflowFailureError as exc:
            response["error"] = str(exc.cause or exc)

    return response


@app.api_route("/process-fines/", methods=["GET", "POST"])
async def process_fines(file_path: str, operation_type: OperationType = OperationType.freeze):
    payload = StartWorkflowRequest(file_path=file_path, operation_type=operation_type)
    workflow_id = f"process-csv-{payload.operation_type.value}-{uuid4()}"
    client = await _get_temporal_client()
    handle = await client.start_workflow(
        ProcessCSVWorkflow.run,
        {
            "file_path": payload.file_path,
            "operation_type": payload.operation_type.value,
            "batch_size": BATCH_SIZE,
        },
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )
    return {"workflow_id": handle.id, "status": "started"}
