from temporalio.worker import Worker

from .activities import scraper_activities
from .temporal_client import get_temporal_client
from .workflows import scraper_workflows


async def run_worker():
    client = await get_temporal_client()

    worker = Worker(
        client,
        task_queue="scraper-tasks",
        workflows=scraper_workflows,
        activities=scraper_activities,
        max_concurrent_activities=5,
    )
    print("Worker started")
    await worker.run()
    print("Worker finished")
