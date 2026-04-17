from temporalio import workflow
from app.workflows.activity_options import ACTIVITY_RETRY_POLICY, VALIDATION_RETRY_POLICY

with workflow.unsafe.imports_passed_through():
    from app.config import BATCH_SIZE
    from app.activities import (
        csv_read_activity,
        file_validation_activity,
        postgres_lookup_activity,
        hold_amount_activity,
        audit_activity,
    )

CSV_ACTIVITY_TIMEOUT_SECONDS = 60
DEFAULT_ACTIVITY_TIMEOUT_SECONDS = 30


@workflow.defn
class ProcessCSVWorkflow:
    @workflow.run
    async def run(self, input_data: dict):
        workflow_id = workflow.info().workflow_id
        file_path = input_data["file_path"]
        operation_type = input_data["operation_type"]
        batch_size = int(input_data.get("batch_size", BATCH_SIZE))
        summary = {
            "workflow_id": workflow_id,
            "file_path": file_path,
            "operation_type": operation_type,
            "total_records": 0,
            "matched_records": 0,
            "processed_records": 0,
            "missing_accounts": 0,
            "failed_records": 0,
        }

        await workflow.execute_activity(
            audit_activity.write,
            {
                "workflow_id": workflow_id,
                "status": "STARTED",
                "message": f"Started {operation_type} workflow for {file_path}",
            },
            schedule_to_close_timeout=workflow.timedelta(
                seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
            ),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )

        await workflow.execute_activity(
            file_validation_activity.validate,
            {"file_path": file_path},
            schedule_to_close_timeout=workflow.timedelta(
                seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
            ),
            retry_policy=VALIDATION_RETRY_POLICY,
        )

        start_row = 0
        while True:
            batch = await workflow.execute_activity(
                csv_read_activity.read_csv_activity,
                {
                    "file_path": file_path,
                    "start_row": start_row,
                    "batch_size": batch_size,
                },
                schedule_to_close_timeout=workflow.timedelta(
                    seconds=CSV_ACTIVITY_TIMEOUT_SECONDS
                ),
                retry_policy=ACTIVITY_RETRY_POLICY,
            )

            records = batch["records"]
            if not records:
                break

            records = await workflow.execute_activity(
                file_validation_activity.validate,
                records,
                schedule_to_close_timeout=workflow.timedelta(
                    seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
                ),
                retry_policy=VALIDATION_RETRY_POLICY,
            )
            summary["total_records"] += len(records)

            for record in records:
                record["workflow_id"] = workflow_id
                record["operation_id"] = (
                    f"{workflow_id}:{operation_type}:row-{record['_row_number']}"
                )

                account = await workflow.execute_activity(
                    postgres_lookup_activity.lookup,
                    record,
                    schedule_to_close_timeout=workflow.timedelta(
                        seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
                    ),
                    retry_policy=ACTIVITY_RETRY_POLICY,
                )
                if not account:
                    summary["missing_accounts"] += 1
                    await workflow.execute_activity(
                        audit_activity.write,
                        {
                            "workflow_id": workflow_id,
                            "operation_id": record["operation_id"],
                            "row_number": record["_row_number"],
                            "cif_id": record.get("cif_id"),
                            "status": "ACCOUNT_NOT_FOUND",
                            "message": "No matching bank account found",
                        },
                        schedule_to_close_timeout=workflow.timedelta(
                            seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
                        ),
                        retry_policy=ACTIVITY_RETRY_POLICY,
                    )
                    continue

                summary["matched_records"] += 1
                activity_fn = (
                    hold_amount_activity.hold
                    if operation_type == "freeze"
                    else hold_amount_activity.unhold
                )
                try:
                    result = await workflow.execute_activity(
                        activity_fn,
                        record,
                        schedule_to_close_timeout=workflow.timedelta(
                            seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
                        ),
                        retry_policy=ACTIVITY_RETRY_POLICY,
                    )
                    summary["processed_records"] += 1
                    await workflow.execute_activity(
                        audit_activity.write,
                        {
                            "workflow_id": workflow_id,
                            "operation_id": record["operation_id"],
                            "row_number": record["_row_number"],
                            "cif_id": record.get("cif_id"),
                            "status": "SUCCESS",
                            "message": (
                                f"{operation_type} applied; "
                                f"duplicate={result.get('duplicate', False)}"
                            ),
                        },
                        schedule_to_close_timeout=workflow.timedelta(
                            seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
                        ),
                        retry_policy=ACTIVITY_RETRY_POLICY,
                    )
                except Exception as exc:
                    summary["failed_records"] += 1
                    await workflow.execute_activity(
                        audit_activity.write,
                        {
                            "workflow_id": workflow_id,
                            "operation_id": record["operation_id"],
                            "row_number": record["_row_number"],
                            "cif_id": record.get("cif_id"),
                            "status": "FAILED",
                            "message": str(exc),
                        },
                        schedule_to_close_timeout=workflow.timedelta(
                            seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
                        ),
                        retry_policy=ACTIVITY_RETRY_POLICY,
                    )

            start_row = batch["next_row"]
            if not batch["has_more"]:
                break

        await workflow.execute_activity(
            audit_activity.write,
            {
                "workflow_id": workflow_id,
                "status": "COMPLETED",
                "message": f"Completed with summary: {summary}",
            },
            schedule_to_close_timeout=workflow.timedelta(
                seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
            ),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
        return summary


RevenueFileWorkflow = ProcessCSVWorkflow
