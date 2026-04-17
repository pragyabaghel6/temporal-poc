from temporalio import workflow
from app.workflows.activity_options import ACTIVITY_RETRY_POLICY

with workflow.unsafe.imports_passed_through():
    from app.activities import hold_amount_activity

DEFAULT_ACTIVITY_TIMEOUT_SECONDS = 30


@workflow.defn
class HoldAccountAmountWorkflow:
    @workflow.run
    async def run(self, record: dict):
        record.setdefault("workflow_id", workflow.info().workflow_id)
        record.setdefault(
            "operation_id",
            f"{workflow.info().workflow_id}:manual:row-{record.get('_row_number', 0)}",
        )
        await workflow.execute_activity(
            hold_amount_activity.hold,
            record,
            schedule_to_close_timeout=workflow.timedelta(
                seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
            ),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
        # Later trigger unhold when payment confirmed
        await workflow.execute_activity(
            hold_amount_activity.unhold,
            record,
            schedule_to_close_timeout=workflow.timedelta(
                seconds=DEFAULT_ACTIVITY_TIMEOUT_SECONDS
            ),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
