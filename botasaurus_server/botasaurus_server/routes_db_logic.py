from datetime import datetime, timezone
from time import sleep
from typing import Any

from casefy import kebabcase
from sqlalchemy import and_, or_, select

from botasaurus_server.errors import JsonHTTPResponseWithMessage

from .apply_pagination import apply_pagination
from .convert_to_english import convert_unicode_dict_to_ascii_dict
from .db_setup import AsyncSessionMaker, Session
from .download import download_results
from .executor import executor
from .filters import apply_filters
from .models import (
    Task,
    TaskStatus,
    create_task_name,
    isoformat,
    serialize_task,
    serialize_ui_display_task,
    serialize_ui_output_task,
)
from .retry_on_db_error import retry_on_db_error
from .server import Server
from .sorts import apply_sorts
from .task_helper import TaskHelper
from .task_results import TaskResults, create_cache_key
from .validation import (
    create_task_not_found_error,
    deep_clone_dict,
    is_list_of_integers,
    is_string_of_min_length,
    is_valid_positive_integer,
    is_valid_positive_integer_including_zero,
    serialize,
    validate_download_params,
    validate_results_request,
    validate_scraper_name,
    validate_task_request,
    validate_ui_patch_task,
)
from .views import _apply_view_for_ui, find_view


@retry_on_db_error
async def perform_create_all_task(
    data, metadata, is_sync, scraper_name, scraper_type, all_task_sort_id
):
    async with AsyncSessionMaker() as session:
        all_task = Task(
            status=TaskStatus.PENDING,
            scraper_name=scraper_name,
            scraper_type=scraper_type,
            is_all_task=True,
            is_sync=is_sync,
            parent_task_id=None,
            data=data,
            meta_data=metadata,
            task_name="All Task",
            sort_id=all_task_sort_id,  # Set the sort_id for the all task
        )
        session.add(all_task)
        await session.commit()

        all_task_id = all_task.id
        return serialize(all_task), all_task_id


@retry_on_db_error
async def perform_create_tasks(tasks, cached_tasks=None) -> list[str]:
    async with AsyncSessionMaker() as session:
        session.add_all(tasks)
        await session.commit()

        if cached_tasks:
            file_list = [
                (
                    t.id,
                    t.result,
                )
                for t in cached_tasks
            ]
            parallel_create_files(file_list)

        # basically here we will write cache, as each cached task adds a property called __cached__result = None [default]
        # but it set when going over cache
        return serialize(tasks)


@retry_on_db_error
def perform_complete_task(all_task_id, remove_duplicates_by):
    executor.complete_as_much_all_task_as_possible(all_task_id, remove_duplicates_by)


@retry_on_db_error
def is_task_done(task_id):
    with Session() as session:
        x = TaskHelper.is_task_completed_or_failed(session, task_id)
    return x


def create_task_query(ets, session):
    return session.query(Task).with_entities(*ets)


@retry_on_db_error
def queryTasks(ets, with_results, page=None, per_page=None, serializer=serialize_task):
    with Session() as session:
        tasks_query = create_task_query(ets, session)
        total_count = tasks_query.count()

        if per_page is None:
            per_page = 1 if total_count == 0 else total_count
            page = 1
        else:
            per_page = int(per_page)

        total_pages = max((total_count + per_page - 1) // per_page, 1)

        page = int(page)
        page = max(min(page, total_pages), 1)  # Ensure page is within valid range

        # Apply pagination if page and per_page are provided and valid
        tasks_query = create_task_query(ets, session).order_by(Task.sort_id.desc())
        if per_page is not None:
            per_page = int(per_page)
            start = (page - 1) * per_page
            tasks_query = tasks_query.limit(per_page).offset(start)

        tasks = tasks_query.all()

        current_page = page if page is not None else 1
        next_page = (
            current_page + 1 if (current_page * per_page) < total_count else None
        )
        previous_page = current_page - 1 if current_page > 1 else None

        if next_page:
            next_page = create_page_url(next_page, per_page, with_results)

        if previous_page:
            previous_page = create_page_url(previous_page, per_page, with_results)

        return {
            "count": total_count,
            "total_pages": total_pages,
            "next": next_page,
            "previous": previous_page,
            "results": [serializer(task, with_results) for task in tasks],
        }


@retry_on_db_error
async def get_task_from_db(task_id):
    async with AsyncSessionMaker() as session:
        task = await TaskHelper.get_task(session, task_id)
        if task:
            return serialize(task)
        else:
            raise create_task_not_found_error(task_id)


@retry_on_db_error
def perform_is_any_task_finished(pending_task_ids, progress_task_ids, all_tasks):
    with Session() as session:
        all_tasks_query = [
            and_(Task.id == x["id"], Task.result_count > x["result_count"])
            for x in all_tasks
        ]
        is_any_task_finished = (
            session.query(Task.id)
            .filter(
                or_(
                    and_(
                        Task.id.in_(pending_task_ids), Task.status != TaskStatus.PENDING
                    ),
                    and_(
                        Task.id.in_(progress_task_ids),
                        Task.status != TaskStatus.IN_PROGRESS,
                    ),
                    *all_tasks_query,
                )
            )
            .first()
            is not None
        )

    return is_any_task_finished


@retry_on_db_error
def perform_is_task_updated(task_id):
    with Session() as session:
        task_data = (
            session.query(Task.updated_at, Task.status)
            .filter(Task.id == task_id)
            .first()
        )
    return task_data


@retry_on_db_error
async def perform_get_task_results(task_id):
    async with AsyncSessionMaker() as session:
        tasks = await TaskHelper.get_tasks_with_entities(
            session,
            [task_id],
            [Task.scraper_name, Task.result_count, Task.is_all_task, Task.data],
        )
        task = tasks[0] if tasks else None
        if not task:
            raise create_task_not_found_error(task_id)

    return task.scraper_name, task.is_all_task, task.data, task.result_count


@retry_on_db_error
def perform_download_task_results(task_id):
    with Session() as session:
        tasks = TaskHelper.get_tasks_with_entities(
            session,
            [task_id],
            [
                Task.scraper_name,
                Task.is_all_task,
                Task.data,
                Task.is_all_task,
                Task.task_name,
            ],
        )
        task = tasks[0] if tasks else None
        if not task:
            raise create_task_not_found_error(task_id)

    return (
        task.scraper_name,
        (
            TaskResults.get_all_task(task_id)
            if task.is_all_task
            else TaskResults.get_task(task_id)
        ),
        task.data,
        task.is_all_task,
        task.task_name,
    )


@retry_on_db_error
async def perform_get_ui_task_results(task_id):
    async with AsyncSessionMaker() as session:
        tasks = await TaskHelper.get_tasks_with_entities(
            session,
            [task_id],
            [
                Task.scraper_name,
                Task.result_count,
                Task.is_all_task,
                Task.data,
                Task.updated_at,
                Task.status,
            ],
        )
        task = tasks[0] if tasks else None
        if not task:
            raise create_task_not_found_error(task_id)

        scraper_name = task.scraper_name
        task_data = task.data
        is_all_task = task.is_all_task
        result_count = task.result_count
        serialized_task = serialize_ui_display_task(task)

    return scraper_name, is_all_task, serialized_task, task_data, result_count


@retry_on_db_error
async def perform_patch_task(action, task_id):
    async with AsyncSessionMaker() as session:
        query = select(
            Task.id,
            Task.is_all_task,
            Task.parent_task_id,
            Task.scraper_name,
        ).where(Task.id == task_id)
        result = await session.execute(query)
        task = result.first()
    if task:
        remove_duplicates_by = Server.get_remove_duplicates_by(task[-1])
        task = task[0:3]
        if action == "delete":
            await delete_task(*task, remove_duplicates_by)
        elif action == "abort":
            await abort_task(*task, remove_duplicates_by)

    await session.commit()


OK_MESSAGE = {"message": "OK"}


async def create_async_task(validated_data):
    scraper_name, data, metadata = validated_data

    tasks_with_all_task, tasks, split_task = await create_tasks(
        Server.get_scraper(scraper_name), data, metadata, False
    )

    if split_task:
        return tasks_with_all_task
    else:
        return tasks[0]


async def execute_async_task(json_data):
    result = await create_async_task(validate_task_request(json_data))
    return result


async def execute_async_tasks(json_data):
    validated_data_items = [validate_task_request(item) for item in json_data]
    result = [
        await create_async_task(validated_data_item)
        for validated_data_item in validated_data_items
    ]
    return result


async def create_tasks(scraper, data, metadata, is_sync):
    create_all_tasks = scraper["create_all_task"]
    split_task = scraper["split_task"]
    scraper_name = scraper["scraper_name"]
    scraper_type = scraper["scraper_type"]
    get_task_name = scraper["get_task_name"]

    # create enough space
    all_task_sort_id = int(datetime.now(timezone.utc).timestamp())
    all_task = None
    all_task_id = None

    if split_task:
        tasks_data = split_task(deep_clone_dict(data))
        if len(tasks_data) == 0:
            return [], [], split_task
    else:
        tasks_data = [data]
    if create_all_tasks:
        all_task, all_task_id = await perform_create_all_task(
            data, metadata, is_sync, scraper_name, scraper_type, all_task_sort_id
        )

    def createTask(task_data, sort_id):
        task_name = get_task_name(task_data) if get_task_name else None
        return Task(
            status=TaskStatus.PENDING,
            scraper_name=scraper_name,
            task_name=task_name,
            scraper_type=scraper_type,
            is_all_task=False,
            is_sync=is_sync,
            parent_task_id=all_task_id,
            data=task_data,
            meta_data=metadata,
            sort_id=sort_id,  # Set the sort_id for the child task
        )

    def create_cached_tasks(
        task_datas: list[dict[str, Any]],
    ) -> tuple[list[Task], list[Task], int]:
        ls: dict[str, Any] = []
        cache_keys: list[str] = []

        for t in task_datas:
            key = create_cache_key(scraper_name, t)
            ls.append({"key": key, "task_data": t})
            cache_keys.append(key)

        cache_map = create_cache_details(cache_keys)

        def create_cached_task(task_data, cached_result, sort_id):
            now_time = datetime.now()
            task_name = get_task_name(task_data) if get_task_name else None
            return Task(
                status=TaskStatus.COMPLETED,
                scraper_name=scraper_name,
                task_name=task_name,
                scraper_type=scraper_type,
                is_all_task=False,
                is_sync=is_sync,
                parent_task_id=all_task_id,
                data=task_data,
                meta_data=metadata,
                result_count=len(cached_result),
                started_at=now_time,
                finished_at=now_time,
                sort_id=sort_id,  # Set the sort_id for the cached task
            )

        tasks: list[Task] = []
        cached_tasks: list[Task] = []
        for idx, item in enumerate(ls):
            cached_result = cache_map.get(item["key"])
            if cached_result:
                sort_id = all_task_sort_id - (idx + 1)
                ts = create_cached_task(item["task_data"], cached_result, sort_id)
                ts.result = cached_result
                tasks.append(ts)
                cached_tasks.append(ts)
            else:
                sort_id = all_task_sort_id - (idx + 1)
                tasks.append(createTask(item["task_data"], sort_id))

        return tasks, cached_tasks

    if Server.cache:
        tasks, cached_tasks = create_cached_tasks(tasks_data)
        tasks = await perform_create_tasks(tasks, cached_tasks)
    else:
        tasks = []
        for idx, task_data in enumerate(tasks_data):
            # Assign sort_id for the non-cached task
            sort_id = all_task_sort_id - (idx + 1)
            tasks.append(createTask(task_data, sort_id))
        tasks = await perform_create_tasks(tasks)

    # here write results with help of cachemap
    if cached_tasks:
        if len(tasks) == len(cached_tasks):
            if all_task_id:
                first_started_at = cached_tasks[0].started_at
                async with AsyncSessionMaker() as session:
                    TaskHelper.update_task(
                        session,
                        all_task_id,
                        {
                            "started_at": first_started_at,
                            "finished_at": first_started_at,
                        },
                    )
                    await session.commit()
                all_task["started_at"] = isoformat(first_started_at)
                all_task["finished_at"] = isoformat(first_started_at)
            print("All tasks results are from cache")
        else:
            print(f"{len(cached_tasks)} out of {len(tasks)} results are from cache")

        if all_task_id:
            await perform_complete_task(
                all_task_id, Server.get_remove_duplicates_by(scraper_name)
            )

    tasks_with_all_task = tasks
    if all_task_id:
        tasks_with_all_task = [all_task] + tasks

    return tasks_with_all_task, tasks, split_task


def save(x):
    """Copy a file from source to destination."""
    TaskResults.save_task(
        x[0],
        x[1],
    )


def parallel_create_files(file_list):
    """

    Copy files in parallel.

    Parameters:
    file_list (list of dict): List of dictionaries with 'source_file' and 'destination_file' keys.
    """
    from joblib import Parallel, delayed

    Parallel(n_jobs=-1)(delayed(save)(file) for file in file_list)


def create_cache_details(cache_keys):
    existing_items = TaskResults.filter_items_in_cache(cache_keys)

    cache_items = TaskResults.get_cached_items_json_filed(existing_items)
    cache_map = {cache["key"]: cache["result"] for cache in cache_items}
    return cache_map


@retry_on_db_error
async def refetch_tasks(item):
    async with AsyncSessionMaker() as session:
        if isinstance(item, list):
            ids = [i["id"] for i in item]
            tasks = await TaskHelper.get_tasks_by_ids(session, ids)
            return serialize(tasks)
        else:
            task = await TaskHelper.get_task(session, item["id"])
            return serialize(task)


def execute_sync_task(json_data):
    scraper_name, data, metadata = validate_task_request(json_data)

    tasks_with_all_task, tasks, split_task = create_tasks(
        Server.get_scraper(scraper_name), data, metadata, True
    )

    if tasks_with_all_task and tasks_with_all_task[0]["is_all_task"]:
        wait_tasks = [tasks_with_all_task[0]]
    else:
        wait_tasks = tasks

    for task in wait_tasks:
        task_id = task["id"]
        while True:
            if is_task_done(task_id):
                break
            sleep(0.1)

    if split_task:
        final = refetch_tasks(tasks_with_all_task)
    else:
        final = refetch_tasks(tasks[0])
    return final


def execute_sync_tasks(json_data):
    validated_data_items = [validate_task_request(item) for item in json_data]

    ts = []
    for validated_data_item in validated_data_items:
        scraper_name, data, metadata = validated_data_item
        ts.append(create_tasks(Server.get_scraper(scraper_name), data, metadata, True))

        # wait for completion
    for t in ts:
        tasks_with_all_task, tasks, split_task = t

        if tasks_with_all_task and tasks_with_all_task[0]["is_all_task"]:
            wait_tasks = [tasks_with_all_task[0]]
        else:
            wait_tasks = tasks

        for task in wait_tasks:
            task_id = task["id"]
            while True:
                if is_task_done(task_id):
                    break
                sleep(0.1)
        # give results
    rst = []
    for t in ts:
        tasks_with_all_task, tasks, split_task = t

        if split_task:
            rst.append(refetch_tasks(tasks_with_all_task))
        else:
            rst.append(refetch_tasks(tasks[0]))
    return rst


def get_ets(with_results):
    return [
        Task.id,
        Task.status,
        Task.task_name,
        Task.scraper_name,
        Task.result_count,
        Task.scraper_type,
        Task.is_all_task,
        Task.is_sync,
        Task.parent_task_id,
        Task.data,
        Task.meta_data,
        Task.finished_at,
        Task.started_at,
        Task.created_at,
        Task.updated_at,
    ]


def create_page_url(page, per_page, with_results):
    query_params = {}

    if page:
        query_params["page"] = page

    if per_page is not None:
        query_params["per_page"] = per_page

    if not with_results:
        query_params["with_results"] = False

    if query_params != {}:
        return query_params


def execute_get_tasks(query_params):
    with_results = query_params.get("with_results", "true").lower() == "true"

    page = query_params.get("page")
    per_page = query_params.get("per_page")

    if per_page is not None:
        if not is_valid_positive_integer(per_page):
            raise JsonHTTPResponseWithMessage(
                "Invalid 'per_page' parameter. It must be a positive integer."
            )
    else:
        page = 1
        per_page = None
    # Validate page and per_page
    if page is not None:
        # Check if any pagination parameter is provided
        if not is_valid_positive_integer(page):
            raise JsonHTTPResponseWithMessage(
                "Invalid 'page' parameter. It must be a positive integer."
            )
    else:
        page = 1

    return queryTasks(get_ets(with_results), with_results, page, per_page)


def is_valid_all_tasks(tasks):
    if not isinstance(tasks, list):
        return False

    for task in tasks:
        if not isinstance(task, dict):
            return False

        if not is_valid_positive_integer(task.get("id")):
            return False
        else:
            task["id"] = int(task["id"])

        if not is_valid_positive_integer_including_zero(task.get("result_count")):
            return False
        else:
            task["result_count"] = int(task["result_count"])
    return True


empty = {
    "count": 0,
    "total_pages": 0,
    "next": None,
    "previous": None,
}


def get_first_view(scraper_name):
    views = Server.get_view_ids(scraper_name)
    if views:
        return views[0]
    return None


def clean_results(
    scraper_name,
    results,
    input_data,
    filters,
    sort,
    view,
    page,
    per_page,
    result_count,
    contains_list_field,
):
    # Apply sorts, filters, and view
    results = apply_sorts(results, sort, Server.get_sorts(scraper_name))
    results = apply_filters(results, filters, Server.get_filters(scraper_name))
    results, hidden_fields = _apply_view_for_ui(
        results, view, Server.get_views(scraper_name), input_data
    )
    if contains_list_field or filters:
        result_count = len(results)
    results = apply_pagination(results, page, per_page, hidden_fields, result_count)
    return results


async def execute_get_task_results(task_id, json_data):
    scraper_name, is_all_task, task_data, result_count = await perform_get_task_results(
        task_id
    )
    validate_scraper_name(scraper_name)
    filters, sort, view, page, per_page = validate_results_request(
        json_data,
        Server.get_sort_ids(scraper_name),
        Server.get_view_ids(scraper_name),
        Server.get_default_sort(scraper_name),
    )
    contains_list_field, results = retrieve_task_results(
        task_id, scraper_name, is_all_task, view, filters, sort, page, per_page
    )
    if not isinstance(results, list):
        results = {**empty, "results": results}
    else:
        results = clean_results(
            scraper_name,
            results,
            task_data,
            filters,
            sort,
            view,
            page,
            per_page,
            result_count,
            contains_list_field,
        )
        del results["hidden_fields"]
    return results


def generate_filename(task_id, view, is_all_task, task_name):
    if view:
        view = kebabcase(view)

    if is_all_task:
        if view:
            return f"all-task-{task_id}-{view}"  # Fixed missing f-string prefix
        else:
            return f"all-task-{task_id}"  # Fixed missing f-string prefix
    else:
        task_name = kebabcase(create_task_name(task_name, task_id))
        if view:
            return f"{task_name}-{view}"
        else:
            return f"{task_name}"


def execute_task_results(task_id, json_data):
    scraper_name, results, task_data, is_all_task, task_name = (
        perform_download_task_results(task_id)
    )
    validate_scraper_name(scraper_name)
    if not isinstance(results, list):
        raise JsonHTTPResponseWithMessage("No Results")

    fmt, filters, sort, view, convert_to_english = validate_download_params(
        json_data,
        Server.get_sort_ids(scraper_name),
        Server.get_view_ids(scraper_name),
        Server.get_default_sort(scraper_name),
    )
    # Apply sorts, filters, and view
    results = apply_sorts(results, sort, Server.get_sorts(scraper_name))
    results = apply_filters(results, filters, Server.get_filters(scraper_name))
    results, _ = _apply_view_for_ui(
        results, view, Server.get_views(scraper_name), task_data
    )

    if convert_to_english:
        results = convert_unicode_dict_to_ascii_dict(results)

    filename = generate_filename(task_id, view, is_all_task, task_name)

    return download_results(results, fmt, filename)


async def delete_task(task_id, is_all_task, parent_id, remove_duplicates_by):
    async with AsyncSessionMaker() as session:
        if is_all_task:
            await TaskHelper.delete_child_tasks(session, task_id)
            await session.commit()
        else:
            if parent_id:
                all_children_count = await TaskHelper.get_all_children_count(
                    session, parent_id, task_id
                )

                if all_children_count == 0:
                    await TaskHelper.delete_task(session, parent_id, True)
                else:
                    has_executing_tasks = (
                        await TaskHelper.get_pending_or_executing_child_count(
                            session, parent_id, task_id
                        )
                    )

                    if not has_executing_tasks:
                        aborted_children_count = (
                            await TaskHelper.get_aborted_children_count(
                                session, parent_id, task_id
                            )
                        )

                        if aborted_children_count == all_children_count:
                            TaskHelper.abort_task(session, parent_id)
                        else:
                            failed_children_count = (
                                await TaskHelper.get_failed_children_count(
                                    session, parent_id, task_id
                                )
                            )
                            if failed_children_count:
                                await TaskHelper.collect_and_save_all_task(
                                    session,
                                    parent_id,
                                    task_id,
                                    remove_duplicates_by,
                                    TaskStatus.FAILED,
                                )
                            else:
                                await TaskHelper.collect_and_save_all_task(
                                    session,
                                    parent_id,
                                    task_id,
                                    remove_duplicates_by,
                                    TaskStatus.COMPLETED,
                                )

    async with AsyncSessionMaker() as session:
        await TaskHelper.delete_task(session, task_id, is_all_task)
        await session.commit()


async def abort_task(task_id, is_all_task, parent_id, remove_duplicates_by):
    async with AsyncSessionMaker() as session:
        if is_all_task:
            await TaskHelper.abort_child_tasks(session, task_id)
        else:
            if parent_id:
                all_children_count = await TaskHelper.get_all_children_count(
                    session, parent_id, task_id
                )

                if all_children_count == 0:
                    await TaskHelper.abort_task(session, parent_id)
                else:
                    has_executing_tasks = (
                        await TaskHelper.get_pending_or_executing_child_count(
                            session, parent_id, task_id
                        )
                    )

                    if not has_executing_tasks:
                        aborted_children_count = (
                            await TaskHelper.get_aborted_children_count(
                                session, parent_id, task_id
                            )
                        )

                        if aborted_children_count == all_children_count:
                            await TaskHelper.abort_task(session, parent_id)
                        else:
                            failed_children_count = (
                                await TaskHelper.get_failed_children_count(
                                    session, parent_id, task_id
                                )
                            )
                            if failed_children_count:
                                await TaskHelper.collect_and_save_all_task(
                                    session,
                                    parent_id,
                                    task_id,
                                    remove_duplicates_by,
                                    TaskStatus.FAILED,
                                )
                            else:
                                await TaskHelper.collect_and_save_all_task(
                                    session,
                                    parent_id,
                                    task_id,
                                    remove_duplicates_by,
                                    TaskStatus.COMPLETED,
                                )
        await session.commit()

    async with AsyncSessionMaker() as session:
        await TaskHelper.abort_task(session, task_id)
        await session.commit()


def execute_get_api_config():
    scrapers = Server.get_scrapers_config()
    config = Server.get_config()
    result = {**config, "scrapers": scrapers}
    return result


def execute_is_any_task_finished(json_data):
    if not is_list_of_integers(json_data.get("pending_task_ids")):
        raise JsonHTTPResponseWithMessage(
            "'pending_task_ids' must be a list of integers"
        )
    if not is_list_of_integers(json_data.get("progress_task_ids")):
        raise JsonHTTPResponseWithMessage(
            "'progress_task_ids' must be a list of integers"
        )

    if not is_valid_all_tasks(json_data.get("all_tasks")):
        raise JsonHTTPResponseWithMessage(
            "'all_tasks' must be a list of dictionaries with 'id' and 'result_count' keys"
        )

    pending_task_ids = json_data["pending_task_ids"]
    progress_task_ids = json_data["progress_task_ids"]
    all_tasks = json_data["all_tasks"]

    is_any_task_finished = perform_is_any_task_finished(
        pending_task_ids, progress_task_ids, all_tasks
    )

    result = {"result": is_any_task_finished}
    return result


def execute_is_task_updated(json_data):
    task_id = json_data.get("task_id")
    last_updated_str = json_data.get("last_updated")
    query_status = json_data.get("status")  # Extract the 'status' parameter

    # Validate 'task_id' using is_valid_integer
    if not is_valid_positive_integer(task_id):
        raise JsonHTTPResponseWithMessage("'task_id' must be a valid integer")

    # Validate 'task_id' using is_valid_integer
    if not is_string_of_min_length(query_status):
        raise JsonHTTPResponseWithMessage(
            "'status' must be a string with at least one character"
        )

    # Convert 'task_id' to integer
    task_id = int(task_id)

    # Parse 'last_updated' using fromisoformat
    try:
        last_updated = datetime.fromisoformat(last_updated_str)  # Strip 'Z' if present
        last_updated = last_updated.replace(
            tzinfo=None
        )  # Make 'last_updated' naive for comparison
    except ValueError:
        raise JsonHTTPResponseWithMessage(
            "'last_updated' must be in valid ISO 8601 format"
        )

    # Query the database for the task's 'updated_at' timestamp using the given 'task_id'
    task_data = perform_is_task_updated(task_id)

    if not task_data:
        raise create_task_not_found_error(task_id)

    task_updated_at, task_status = task_data
    task_updated_at = task_updated_at.replace(
        tzinfo=None
    )  # Make 'task_updated_at' naive for comparison

    if (task_updated_at > last_updated) or (task_status != query_status):
        is_updated = True
    else:
        is_updated = False

    result = {"result": is_updated}
    return result


output_ui_tasks_ets = [
    Task.id,
    Task.status,
    Task.task_name,
    Task.result_count,
    Task.is_all_task,
    Task.finished_at,
    Task.started_at,
]


def execute_get_ui_tasks(page):
    # Validate page and per_page
    if page is not None:
        # Check if any pagination parameter is provided
        if not is_valid_positive_integer(page):
            raise JsonHTTPResponseWithMessage(
                "Invalid 'page' parameter. It must be a positive integer."
            )
    else:
        page = 1

    return queryTasks(output_ui_tasks_ets, False, page, 100, serialize_ui_output_task)


async def execute_patch_task(page, json_data):
    if not (is_valid_positive_integer(page)):
        raise JsonHTTPResponseWithMessage(
            "Invalid 'page' parameter. Must be a positive integer.", status=400
        )

    action, task_ids = validate_ui_patch_task(json_data)

    for task_id in task_ids:
        await perform_patch_task(action, task_id)

    result = queryTasks(output_ui_tasks_ets, False, page, 100, serialize_ui_output_task)
    return result


async def execute_get_ui_task_results(task_id, json_data, query_params):
    (
        scraper_name,
        is_all_task,
        serialized_task,
        task_data,
        result_count,
    ) = await perform_get_ui_task_results(task_id)
    validate_scraper_name(scraper_name)

    filters, sort, view, page, per_page = validate_results_request(
        json_data,
        Server.get_sort_ids(scraper_name),
        Server.get_view_ids(scraper_name),
        Server.get_default_sort(scraper_name),
    )

    forceApplyFirstView = (
        query_params.get("force_apply_first_view", "none").lower() == "true"
    )
    if forceApplyFirstView:
        view = get_first_view(scraper_name)

    contains_list_field, results = retrieve_task_results(
        task_id, scraper_name, is_all_task, view, filters, sort, page, per_page
    )

    if not isinstance(results, list):
        final = {**empty, "results": results, "task": serialized_task}
    else:
        results = clean_results(
            scraper_name,
            results,
            task_data,
            filters,
            sort,
            view,
            page,
            per_page,
            result_count,
            contains_list_field,
        )
        results["task"] = serialized_task
        final = results
    return final


def retrieve_task_results(
    task_id, scraper_name, is_all_task, view, filters, sort, page, per_page
):
    contains_list_field = (
        view and find_view(Server.get_views(scraper_name), view).contains_list_field
    )
    if is_all_task:
        if sort or filters or contains_list_field:
            # get all tasks we need to apply
            results = TaskResults.get_all_task(task_id)
        else:
            # if is listish view
            limit = page * per_page if per_page else None
            results = TaskResults.get_all_task(task_id, limit=limit)
    else:
        results = TaskResults.get_task(task_id)
    return contains_list_field, results
