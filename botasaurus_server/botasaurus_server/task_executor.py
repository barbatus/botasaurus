import asyncio
import traceback
from datetime import datetime
from typing import Any, Callable, Optional

from sqlalchemy import and_, delete, or_, select, update

from .cleaners import clean_data
from .db_setup import Session, get_async_session
from .models import Task, TaskStatus, remove_duplicates_by_key
from .retry_on_db_error import retry_on_db_error
from .server import Server, get_scraper_error_message
from .task_helper import TaskHelper


class TaskExecutor:
    @retry_on_db_error
    async def fix_in_progress_tasks(self):
        async with get_async_session() as session:
            await session.execute(
                delete(Task).where(
                    Task.is_sync.is_(True),
                    Task.status.in_(
                        (TaskStatus.PENDING, TaskStatus.IN_PROGRESS),
                    ),
                ),
            )

            await session.execute(
                update(Task)
                .where(Task.status == TaskStatus.IN_PROGRESS)
                .values(
                    status=TaskStatus.PENDING,
                    started_at=None,
                    finished_at=None,
                ),
            )

            await session.commit()

    def get_max_running_count(self, scraper_type):
        return Server.get_rate_limit()[scraper_type]

    @retry_on_db_error
    async def process_tasks(
        self, task_ids: list[int], on_heatbeat: Optional[Callable] = None
    ):
        tasks_json: list[dict[str, Any]] = []
        async with get_async_session() as session:
            stmt = (
                select(Task)
                .where(Task.id.in_(task_ids))
                .order_by(
                    Task.sort_id.desc(),
                    Task.is_sync.desc(),
                )
            )

            tasks = (await session.scalars(stmt)).all()

            if not tasks:
                return

            for task in tasks:
                valid_scraper_names = Server.get_scrapers_names()
                valid_scraper_names_set = set(valid_scraper_names)

                if task.scraper_name not in valid_scraper_names_set:
                    valid_names_string = ", ".join(valid_scraper_names)
                    raise Exception(
                        get_scraper_error_message(
                            valid_scraper_names,
                            task.scraper_name,
                            valid_names_string,
                        )
                    )

            # Collect task and parent IDs
            task_ids = [task.id for task in tasks]
            parent_ids = list(
                {task.parent_task_id for task in tasks if task.parent_task_id}
            )

            # Bulk update the status of tasks
            await session.execute(
                update(Task)
                .where(
                    or_(
                        Task.id.in_(task_ids),
                        and_(
                            Task.id.in_(parent_ids),
                            Task.started_at.is_(None),
                        ),
                    ),
                )
                .values(
                    {
                        "status": TaskStatus.IN_PROGRESS,
                        "started_at": datetime.now(),
                    }
                )
            )

            tasks_json = []
            for task in tasks:
                task_dict = {
                    "id": task.id,
                    "status": task.status,
                    "scraper_name": task.scraper_name,
                    "scraper_type": task.scraper_type,
                    "is_sync": task.is_sync,
                    "parent_task_id": task.parent_task_id,
                    "data": task.data,
                    "metadata": task.meta_data,
                }
                tasks_json.append(task_dict)

            await session.commit()

        await asyncio.gather(
            *(self.run_task(task_json, on_heatbeat) for task_json in tasks_json)
        )

    async def run_task(self, task, on_heatbeat: Optional[Callable]):
        task_id = task["id"]
        scraper_name = task["scraper_name"]
        parent_task_id = task["parent_task_id"]
        metadata = {"metadata": task["metadata"]} if task["metadata"] != {} else {}
        task_data = task["data"]

        fn = Server.get_scraping_function(scraper_name)
        exception_log = None

        try:
            result = await asyncio.to_thread(
                fn,
                task_data,
                **metadata,
                parallel=None,
                cache=False,
                beep=False,
                run_async=False,
                async_queue=False,
                raise_exception=True,
                close_on_crash=True,
                output=None,
                create_error_logs=False,
                on_heatbeat=on_heatbeat,
            )

            result = clean_data(result)

            remove_duplicates_by = Server.get_remove_duplicates_by(scraper_name)
            if remove_duplicates_by:
                result = remove_duplicates_by_key(
                    result,
                    remove_duplicates_by,
                )

            await self.mark_task_as_success(
                task_id,
                result,
                Server.cache,
                scraper_name,
                task_data,
            )
        except Exception as e:
            exception_log = traceback.format_exc()
            traceback.print_exc()
            print("Error in run_task ", e)
            await self.mark_task_as_failure(task_id, exception_log)
        finally:
            if parent_task_id:
                if exception_log:
                    self.update_parent_task(task_id, [])
                else:
                    self.update_parent_task(task_id, result)

    @retry_on_db_error
    def update_parent_task(self, task_id, result):
        with Session() as session:
            task_to_update = session.get(Task, task_id)
            parent_id = task_to_update.parent_task_id
            scraper_name = task_to_update.scraper_name

        if parent_id:
            self.complete_parent_task_if_possible(
                parent_id,
                Server.get_remove_duplicates_by(scraper_name),
                result,
            )

    @retry_on_db_error
    async def complete_parent_task_if_possible(
        self, parent_id, remove_duplicates_by, result
    ):
        async with get_async_session() as session:
            parent_task = await TaskHelper.get_task(
                session,
                parent_id,
                [TaskStatus.PENDING, TaskStatus.IN_PROGRESS],
            )

            if parent_task:
                await TaskHelper.update_parent_task_results(
                    session,
                    parent_id,
                    result,
                )
                is_done = await TaskHelper.are_all_child_task_done(
                    session,
                    parent_id,
                )
                if is_done:
                    failed_children_count = await TaskHelper.get_failed_children_count(
                        session,
                        parent_id,
                    )

                    status = (
                        TaskStatus.FAILED
                        if failed_children_count
                        else TaskStatus.COMPLETED
                    )
                    await TaskHelper.update_task(
                        session,
                        parent_id,
                        {
                            "status": status,
                            "finished_at": datetime.now(),
                        },
                    )
                    await session.commit()
                    # await TaskHelper.read_clean_save_task(
                    #     parent_id, remove_duplicates_by, status
                    # )

    @retry_on_db_error
    async def mark_task_as_failure(self, task_id, exception_log):
        async with get_async_session() as session:
            await TaskHelper.update_task(
                session,
                task_id,
                {
                    "status": TaskStatus.FAILED,
                    "finished_at": datetime.now(),
                    "result": exception_log,
                },
                [TaskStatus.IN_PROGRESS],
            )
            await session.commit()

    @retry_on_db_error
    async def mark_task_as_success(
        self, task_id, result, cache_task, scraper_name, data
    ):
        async with get_async_session() as session:
            await TaskHelper.update_task(
                session,
                task_id,
                {
                    "result_count": len(result),
                    "status": TaskStatus.COMPLETED,
                    "finished_at": datetime.now(),
                    "result": result,
                },
                [TaskStatus.IN_PROGRESS],
            )
            await session.commit()
