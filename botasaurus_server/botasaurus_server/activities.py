import asyncio
from typing import Any, Optional

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .executor import executor


class NonRetryableError(ApplicationError):
    def __init__(self, message: str, *details: Any, type: Optional[str] = None):
        super().__init__(
            message,
            *details,
            type="NonRetryableError",
            non_retryable=True,
        )


@activity.defn(name="run_scraper")
async def run_scraper(task_id: int) -> None:
    try:
        return await executor.process_tasks([task_id])
    except Exception as e:
        raise NonRetryableError(str(e))


@activity.defn(name="mark_tasks_as_failed")
async def mark_tasks_as_failed(failed_tasks: list[tuple[int, str]]) -> None:
    coroutnines = []
    for task_id, error in failed_tasks:
        coroutnines.append(await executor.mark_task_as_failure(task_id, error))
    return await asyncio.gather(*coroutnines)


scraper_activities = [run_scraper, mark_tasks_as_failed]
