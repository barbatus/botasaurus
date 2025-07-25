import asyncio
import time
import traceback
from datetime import datetime
from threading import Lock
from typing import Any

from sqlalchemy import and_, delete, or_, select, update

from botasaurus.dontcache import is_dont_cache

from .cleaners import clean_data
from .db_setup import AsyncSessionMaker, Session
from .models import Task, TaskStatus, remove_duplicates_by_key
from .retry_on_db_error import retry_on_db_error
from .scraper_type import ScraperType
from .server import Server, get_scraper_error_message
from .task_helper import TaskHelper
from .task_results import TaskResults


class TaskExecutor:
    current_capacity = {"browser": 0, "request": 0, "task": 0}
    lock = Lock()

    async def start(self):
        await self.fix_in_progress_tasks()
        await self.task_worker()

    @retry_on_db_error
    async def fix_in_progress_tasks(self):
        async with AsyncSessionMaker() as session:
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

    async def task_worker(self):
        browser_scrapers = len(Server.get_browser_scrapers()) > 0
        request_scrapers = len(Server.get_request_scrapers()) > 0
        task_scrapers = len(Server.get_task_scrapers()) > 0

        while True:
            try:
                if browser_scrapers:
                    await self.process_tasks(ScraperType.BROWSER)

                if request_scrapers:
                    await self.process_tasks(ScraperType.REQUEST)

                if task_scrapers:
                    await self.process_tasks(ScraperType.TASK)
            except Exception:
                traceback.print_exc()

            time.sleep(1)

    async def process_tasks(self, scraper_type):
        rate_limit = self.get_max_running_count(scraper_type)
        current_count = self.get_current_running_count(scraper_type)
        if current_count < rate_limit:
            await self.execute_pending_tasks(
                and_(
                    Task.status == TaskStatus.PENDING,
                    Task.scraper_type == scraper_type,
                    Task.is_all_task.is_(False),
                ),
                current_count,
                rate_limit,
                scraper_type,
            )

    def get_max_running_count(self, scraper_type):
        return Server.get_rate_limit()[scraper_type]

    def get_current_running_count(self, scraper_type):
        return self.current_capacity[scraper_type]

    @retry_on_db_error
    async def execute_pending_tasks(self, task_filter, current, limit, scraper_type):
        tasks_json: list[dict[str, Any]] = []
        async with AsyncSessionMaker() as session:
            stmt = (
                select(Task)
                .where(task_filter)
                .order_by(
                    Task.sort_id.desc(),
                    Task.is_sync.desc(),
                )
            )
            if limit is not None:
                stmt = stmt.limit(max(limit - current, 0))

            tasks: list[Task] = (await session.scalars(stmt)).all()

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
                        and_(Task.id.in_(parent_ids), Task.started_at.is_(None)),
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
                    "is_all_task": task.is_all_task,
                    "is_sync": task.is_sync,
                    "parent_task_id": task.parent_task_id,
                    "data": task.data,
                    "metadata": task.meta_data,
                }
                tasks_json.append(task_dict)

            await session.commit()

        await asyncio.gather(
            *[
                self.run_task_and_update_state(
                    scraper_type,
                    task_json,
                )
                for task_json in tasks_json
            ]
        )

    async def run_task_and_update_state(self, scraper_type, task_json):
        await self.run_task(task_json)

        # Update capacity bookkeeping.
        self.increment_capacity(scraper_type)

    def increment_capacity(self, scraper_type):
        # Temp Lock Disabling to check if it reduces the deadlock on vm tasks
        # with self.lock:
        self.current_capacity[scraper_type] += 1

    def decrement_capcity(self, scraper_type):
        # Temp Lock Disabling to check if it reduces the deadlock on vm tasks
        # with self.lock:
        self.current_capacity[scraper_type] -= 1

    async def run_task(self, task):
        scraper_type = task["scraper_type"]
        task_id = task["id"]
        scraper_name = task["scraper_name"]
        parent_task_id = task["parent_task_id"]
        metadata = {"metadata": task["metadata"]} if task["metadata"] != {} else {}
        task_data = task["data"]

        fn = Server.get_scraping_function(scraper_name)
        exception_log = None
        try:
            try:
                # Run the (potentially blocking) scraping function in a separate
                # thread so it does not block the event loop.
                result = await asyncio.to_thread(
                    fn,
                    task_data,
                    **metadata,
                    # parallel=None,
                    # cache=False,
                    # beep=False,
                    # run_async=False,
                    # async_queue=False,
                    raise_exception=True,
                    close_on_crash=True,
                    output=None,
                    create_error_logs=False,
                    return_dont_cache_as_is=True,
                )
                if is_dont_cache(result):
                    is_result_dont_cached = True
                    # unpack
                    result = result.data
                else:
                    is_result_dont_cached = False

                result = clean_data(result)

                remove_duplicates_by = Server.get_remove_duplicates_by(scraper_name)
                if remove_duplicates_by:
                    result = remove_duplicates_by_key(result, remove_duplicates_by)

                if is_result_dont_cached:
                    await self.mark_task_as_success(
                        task_id,
                        result,
                        False,
                        scraper_name,
                        task_data,
                    )
                else:
                    await self.mark_task_as_success(
                        task_id,
                        result,
                        Server.cache,
                        scraper_name,
                        task_data,
                    )
                self.decrement_capcity(scraper_type)
            except Exception:
                self.decrement_capcity(scraper_type)
                exception_log = traceback.format_exc()
                traceback.print_exc()
                await self.mark_task_as_failure(task_id, exception_log)
            finally:
                if parent_task_id:
                    if exception_log:
                        self.update_parent_task(task_id, [])
                    else:
                        self.update_parent_task(task_id, result)
        except Exception as e:
            traceback.print_exc()
            print("Error in run_task ", e)

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

    async def complete_parent_task_if_possible(
        self, parent_id, remove_duplicates_by, result
    ):
        async with AsyncSessionMaker() as session:
            parent_task = await TaskHelper.get_task(
                session,
                parent_id,
                [TaskStatus.PENDING, TaskStatus.IN_PROGRESS],
            )

            # MAY BE DELETED SO CHECK
            if parent_task:
                # is fast no worry
                await TaskHelper.update_parent_task_results(parent_id, result)
                is_done = await TaskHelper.are_all_child_task_done(session, parent_id)
                if is_done:
                    failed_children_count = await TaskHelper.get_failed_children_count(
                        session, parent_id
                    )

                    status = (
                        TaskStatus.FAILED
                        if failed_children_count
                        else TaskStatus.COMPLETED
                    )
                    await TaskHelper.read_clean_save_task(
                        parent_id, remove_duplicates_by, status
                    )

    async def complete_as_much_all_task_as_possible(
        self, parent_id, remove_duplicates_by
    ):
        async with AsyncSessionMaker() as session:
            is_done = await TaskHelper.are_all_child_task_done(session, parent_id)
            if is_done:
                failed_children_count = await TaskHelper.get_failed_children_count(
                    session, parent_id
                )
                status = (
                    TaskStatus.FAILED if failed_children_count else TaskStatus.COMPLETED
                )
                await TaskHelper.collect_and_save_all_task(
                    session,
                    parent_id,
                    None,
                    remove_duplicates_by,
                    status,
                )
            else:
                await TaskHelper.collect_and_save_all_task(
                    session,
                    parent_id,
                    None,
                    remove_duplicates_by,
                    TaskStatus.IN_PROGRESS,
                )

    @retry_on_db_error
    async def mark_task_as_failure(self, task_id, exception_log):
        TaskResults.save_task(task_id, exception_log)
        async with AsyncSessionMaker() as session:
            await TaskHelper.update_task(
                session,
                task_id,
                {
                    "status": TaskStatus.FAILED,
                    "finished_at": datetime.now(),
                },
                [TaskStatus.IN_PROGRESS],
            )
            await session.commit()

    @retry_on_db_error
    async def mark_task_as_success(
        self, task_id, result, cache_task, scraper_name, data
    ):
        TaskResults.save_task(task_id, result)
        if cache_task:
            TaskResults.save_cached_task(scraper_name, data, result)
        async with AsyncSessionMaker() as session:
            update_result = await TaskHelper.update_task(
                session,
                task_id,
                {
                    "result_count": len(result),
                    "status": TaskStatus.COMPLETED,
                    "finished_at": datetime.now(),
                },
                [TaskStatus.IN_PROGRESS],
            )
            if not update_result:
                # if the task is aborted in progress, there should be no results
                TaskResults.delete_task(task_id)

            await session.commit()
