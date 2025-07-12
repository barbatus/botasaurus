from typing import Dict

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .executor import executor
from .routes_db_logic import OK_MESSAGE

router = APIRouter()


@router.post("/k8s/run-worker-task")
async def k8s_run_worker_task(request: Request) -> Dict[str, str]:
    data = await request.json()
    executor.run_worker_task(data["task"], data["node_name"])
    return JSONResponse(content=OK_MESSAGE)
