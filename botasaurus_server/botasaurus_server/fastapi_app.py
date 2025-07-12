from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

from .routes_db_logic import (
    OK_MESSAGE,
    execute_async_task,
    execute_async_tasks,
    execute_get_api_config,
    execute_get_task_results,
    execute_get_tasks,
    execute_get_ui_task_results,
    execute_get_ui_tasks,
    execute_is_any_task_finished,
    execute_is_task_updated,
    execute_patch_task,
    execute_sync_task,
    execute_sync_tasks,
    execute_task_results,
    get_task_from_db,
    perform_patch_task,
)
from .validation import validate_patch_task
from .env import is_master

app = FastAPI(title="Botasaurus API")

if is_master:
    from .master_routes import router as master_router
    app.include_router(master_router)
else:
    from .worker_routes import router as worker_router
    app.include_router(worker_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def jsonify(data: Any) -> JSONResponse:  # keep compatibility helper
    return JSONResponse(content=data)


def _dict_from_query(request_query) -> Dict[str, str]:
    """Convert FastAPI/Starlette query params to a plain dict[str, str]."""
    return {k: v for k, v in request_query.items()}


@app.get("/", include_in_schema=False)
def home() -> RedirectResponse:
    return RedirectResponse(url="/api")


@app.get("/api", response_model=Dict[str, str])
def api_root() -> Dict[str, str]:
    return JSONResponse(content=OK_MESSAGE)


@app.post("/api/tasks/create-task-async")
async def create_task_async(request: Request):
    json_data = await request.json()
    if isinstance(json_data, list):
        result = await execute_async_tasks(json_data)
    else:
        result = await execute_async_task(json_data)
    return jsonify(result)


@app.post("/api/tasks/create-task-sync")
async def create_task_sync(request: Request):
    json_data = await request.json()
    if isinstance(json_data, list):
        result = execute_sync_tasks(json_data)
    else:
        result = execute_sync_task(json_data)
    return jsonify(result)


@app.get("/api/tasks")
async def get_tasks(request: Request):
    query_dict = _dict_from_query(request.query_params)
    result = execute_get_tasks(query_dict)
    return result


@app.get("/api/tasks/{task_id}")
async def get_task(task_id: int):
    return await get_task_from_db(task_id)


@app.post("/api/tasks/{task_id}/results")
async def get_task_results(task_id: int, request: Request):
    json_data = await request.json()
    results = await execute_get_task_results(task_id, json_data)
    return jsonify(results)


@app.post("/api/tasks/{task_id}/download")
async def download_task_results(task_id: int, request: Request):
    json_data = await request.json()
    return execute_task_results(task_id, json_data)


@app.patch("/api/tasks/{task_id}/abort")
async def abort_single_task(task_id: int):
    await perform_patch_task("abort", task_id)
    return JSONResponse(content=OK_MESSAGE)


@app.delete("/api/tasks/{task_id}")
async def delete_single_task(task_id: int):
    await perform_patch_task("delete", task_id)
    return JSONResponse(content=OK_MESSAGE)


@app.post("/api/tasks/bulk-abort")
async def bulk_abort_tasks(request: Request):
    json_data = await request.json()
    task_ids = validate_patch_task(json_data)
    for task_id in task_ids:
        await perform_patch_task("abort", task_id)
    return JSONResponse(content=OK_MESSAGE)


@app.post("/api/tasks/bulk-delete")
async def bulk_delete_tasks(request: Request):
    json_data = await request.json()
    task_ids = validate_patch_task(json_data)
    for task_id in task_ids:
        await perform_patch_task("delete", task_id)
    return JSONResponse(content=OK_MESSAGE)


@app.get("/api/ui/config")
def get_api_config():
    return execute_get_api_config()


@app.post("/api/ui/tasks/is-any-task-updated")
async def is_any_task_updated(request: Request):
    json_data = await request.json()
    result = execute_is_any_task_finished(json_data)
    return jsonify(result)


@app.post("/api/ui/tasks/is-task-updated")
async def is_task_updated(request: Request):
    json_data = await request.json()
    result = execute_is_task_updated(json_data)
    return jsonify(result)


@app.post("/api/ui/tasks/results")
async def get_ui_tasks_results(request: Request):
    json_data = await request.json()
    query_dict = _dict_from_query(request.query_params)
    task_ids = json_data["task_ids"]
    final = await asyncio.gather(
        *[
            execute_get_ui_task_results(task_id, json_data, query_dict)
            for task_id in task_ids
        ]
    )
    return jsonify(final)


@app.get("/api/ui/tasks")
async def get_tasks_for_ui_display(page: Optional[int] = Query(None)):
    result = execute_get_ui_tasks(page)
    return result


@app.patch("/api/ui/tasks")
async def patch_task(
    request: Request,
    page: Optional[int] = Query(None),
):
    json_data = await request.json()
    result = await execute_patch_task(page, json_data)
    return result


@app.post("/api/ui/tasks/{task_id}/results")
async def get_ui_task_results(task_id: int, request: Request):
    json_data = await request.json()
    query_dict = _dict_from_query(request.query_params)
    final = await execute_get_ui_task_results(task_id, json_data, query_dict)
    return jsonify(final)

