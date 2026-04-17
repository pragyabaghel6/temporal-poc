from datetime import timedelta

from temporalio.common import RetryPolicy

ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
)

VALIDATION_RETRY_POLICY = RetryPolicy(maximum_attempts=1)
CSV_ACTIVITY_TIMEOUT = timedelta(seconds=60)
DEFAULT_ACTIVITY_TIMEOUT = timedelta(seconds=30)
