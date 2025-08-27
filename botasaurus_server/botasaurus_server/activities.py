from temporalio import activity

from .executor import executor


@activity.defn(name="run_scraper")
async def run_scraper(task_id: int) -> None:
    return await executor.process_tasks([task_id])


scraper_activities = [run_scraper]
