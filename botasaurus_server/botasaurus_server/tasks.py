import asyncio

from .celery_worker import celery_app
from .task_executor import TaskExecutor


def get_or_create_event_loop():
    try:
        loop = asyncio.get_running_loop()
        return loop
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
            return loop
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop


executor = TaskExecutor()


@celery_app.task(name="tasks.execute_task")
def execute_task(task_id: int):
    loop = get_or_create_event_loop()
    return loop.run_until_complete(executor.process_tasks([task_id]))
