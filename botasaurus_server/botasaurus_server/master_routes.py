from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .executor import executor
from .routes_db_logic import OK_MESSAGE  # imports all routes as well

router = APIRouter()


@router.post("/k8s/success")
async def k8s_success(request: Request):
    json_data = await request.json()

    task_id = json_data["task_id"]
    task_type = json_data["task_type"]
    task_result = json_data["task_result"]
    scraper_name = json_data["scraper_name"]
    data = json_data["data"]
    node_name = json_data["node_name"]

    executor.on_success(
        task_id, task_type, task_result, node_name, scraper_name, data
    )

    return JSONResponse(content=OK_MESSAGE)


@router.post("/k8s/fail")
async def k8s_fail(request: Request):
    json_data = await request.json()
    task_id = json_data["task_id"]
    task_type = json_data["task_type"]
    task_result = json_data["task_result"]
    node_name = json_data["node_name"]

    executor.on_failure(task_id, task_type, task_result, node_name)

    return JSONResponse(content=OK_MESSAGE)
