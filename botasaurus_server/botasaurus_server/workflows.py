from datetime import timedelta

from temporalio import common, workflow

with workflow.unsafe.imports_passed_through():
    from .activities import mark_tasks_as_failed, run_scraper
    from .utils import execute_concurrently_stat


activity_timeout = timedelta(seconds=300)
heartbeat_timeout = timedelta(seconds=120)
retry_policy = common.RetryPolicy(
    maximum_attempts=3,
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
                workflow.execute_activity(
                    run_scraper,
                    args=[task_id],
                    start_to_close_timeout=activity_timeout,
                    # heartbeat_timeout=heartbeat_timeout,
                    retry_policy=retry_policy,
                )
            )
        _, errors, _ = await execute_concurrently_stat(activities)
        failed_tasks = [
            (task_ids[index], repr(error)) for index, error in errors.items()
        ]
        if failed_tasks:
            await workflow.execute_activity(
                mark_tasks_as_failed,
                args=[
                    failed_tasks,
                ],
                start_to_close_timeout=activity_timeout,
                retry_policy=retry_policy,
            )


scraper_workflows = [ScrapeWorkflow]
