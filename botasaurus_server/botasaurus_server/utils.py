import asyncio
from dataclasses import dataclass
from os import getcwd, path
from time import monotonic
from typing import Any, Awaitable, Coroutine, Dict, List, Sequence, Tuple, TypeVar

from .env import is_master

if is_master:
    path_task_results = path.abspath(path.join(getcwd(), "..", "db", "task_results"))
else:
    path_task_results = path.abspath(path.join(getcwd(), "task_results"))
if is_master:
    path_task_results_tasks = path.abspath(
        path.join(getcwd(), "..", "db", "task_results", "tasks")
    )
else:
    path_task_results_tasks = path.abspath(path.join(getcwd(), "task_results", "tasks"))
if is_master:
    path_task_results_cache = path.abspath(
        path.join(getcwd(), "..", "db", "task_results", "cache")
    )
else:
    path_task_results_cache = path.abspath(path.join(getcwd(), "task_results", "cache"))


TaskResultT = TypeVar("TaskResultT")


async def execute_concurrently(
    awaitables: Sequence[Awaitable[TaskResultT]],
    raise_on_error: bool = False,
) -> Tuple[Dict[int, TaskResultT], Dict[int, Exception]]:
    """
    Run a set of awaitables concurrently and return the tuple of results and errors.

    :param awaitables: a sequence of awaitables to run concurrently
    :return: a tuple containing the results and errors
    """
    results: Dict[int, TaskResultT] = {}
    errors: Dict[int, Exception] = {}
    outcomes = await asyncio.gather(*awaitables, return_exceptions=not raise_on_error)
    for i, outcome in enumerate(outcomes):
        task_name = i
        if isinstance(outcome, Exception):
            errors[task_name] = outcome
        else:
            results[task_name] = outcome

    return results, errors


@dataclass
class GatherStats:
    total_time_sec: float
    success_count: int
    error_count: int

    def __post_init__(self):
        # Round the `total_time_sec` to 2 decimal places on initialization
        self.total_time_sec = round(self.total_time_sec, 2)


async def execute_concurrently_stat(
    coroutines: Sequence[Coroutine[Any, Any, TaskResultT]],
    results_list: bool = False,
    *,
    operation_name: str | None = None,
    raise_on_error: bool = False,
) -> Tuple[
    Dict[int, TaskResultT] | List[TaskResultT],
    Dict[int, Exception],
    GatherStats,
]:
    start_time = monotonic()
    results, errors = await execute_concurrently(
        coroutines,
        raise_on_error=raise_on_error,
    )
    total_time = monotonic() - start_time

    stats = GatherStats(
        total_time_sec=total_time,
        success_count=len(results),
        error_count=len(errors),
    )

    if errors and operation_name:
        for error in errors:
            print(f"Error in {operation_name.lower()}: {errors[error]}")

    if errors and raise_on_error:
        raise next(iter(errors.values()))

    if operation_name:
        print(
            f"{operation_name} stats {stats}",
        )

    return list(results.values()) if results_list is True else results, errors, stats
