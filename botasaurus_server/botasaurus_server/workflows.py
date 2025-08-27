import asyncio
from datetime import timedelta

from temporalio import common, workflow

with workflow.unsafe.imports_passed_through():
    from .activities import run_scraper


activity_timeout = timedelta(seconds=300)
heartbeat_timeout = timedelta(seconds=60)
retry_policy = common.RetryPolicy(
    maximum_attempts=1,
    initial_interval=timedelta(milliseconds=1),
    backoff_coefficient=1.0,
    non_retryable_error_types=["NonRetryableError"],
)
workflow_retry_policy = common.RetryPolicy(
    maximum_attempts=1,
    non_retryable_error_types=["NonRetryableError"],
)


@workflow.defn(name="runScrapeTasks")
class ScrapeWorkflow:
    @workflow.run
    async def run(self, task_ids: list[int]) -> None:
        activities: workflow.ActivityHandle[None] = []
        for task_id in task_ids:
            activities.append(
                workflow.start_activity(
                    run_scraper,
                    args=[task_id],
                    start_to_close_timeout=activity_timeout,
                    heartbeat_timeout=heartbeat_timeout,
                    retry_policy=retry_policy,
                )
            )
        await asyncio.gather(*activities)


scraper_workflows = [ScrapeWorkflow]
