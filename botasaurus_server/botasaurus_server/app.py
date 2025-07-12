import asyncio

import uvicorn

from .env import is_in_kubernetes
from .executor import executor
from .fastapi_app import app as fastapi_app


@fastapi_app.on_event("startup")
async def startup_event():
    asyncio.create_task(executor.start())


def run_backend():
    host = "0.0.0.0" if is_in_kubernetes else "127.0.0.1"

    uvicorn.run(fastapi_app, host=host, port=8000, log_level="info")
